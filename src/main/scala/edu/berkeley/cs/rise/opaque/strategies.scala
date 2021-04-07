/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.rise.opaque

import edu.berkeley.cs.rise.opaque.execution._
import edu.berkeley.cs.rise.opaque.logical._

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftExistence
import org.apache.spark.sql.catalyst.optimizer.{
  BuildLeft,
  BuildRight,
  BuildSide,
  JoinSelectionHelper
}

object OpaqueOperators extends Strategy with JoinSelectionHelper {

  def isEncrypted(plan: LogicalPlan): Boolean = {
    plan.find {
      case _: OpaqueOperator => true
      case _ => false
    }.nonEmpty
  }

  def isEncrypted(plan: SparkPlan): Boolean = {
    plan.find {
      case _: OpaqueOperatorExec => true
      case _ => false
    }.nonEmpty
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Project(projectList, child) if isEncrypted(child) =>
      EncryptedProjectExec(projectList, planLater(child)) :: Nil

    // We don't support null values yet, so there's no point in checking whether the output of an
    // encrypted operator is null
    case p @ Filter(And(IsNotNull(_), IsNotNull(_)), child) if isEncrypted(child) =>
      planLater(child) :: Nil
    case p @ Filter(IsNotNull(_), child) if isEncrypted(child) =>
      planLater(child) :: Nil

    case Filter(condition, child) if isEncrypted(child) =>
      EncryptedFilterExec(condition, planLater(child)) :: Nil

    case Sort(sortExprs, global, child) if isEncrypted(child) =>
      EncryptedSortExec(sortExprs, global, planLater(child)) :: Nil

    // Used to match equi joins
    case p @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right, _)
        if isEncrypted(p) =>
      val (leftProjSchema, leftKeysProj, leftTag) =
        tagForEquiJoin(leftKeys, left.output, isLeftPrimary(joinType))
      val (rightProjSchema, rightKeysProj, rightTag) =
        tagForEquiJoin(rightKeys, right.output, !isLeftPrimary(joinType))

      val (primary, primaryProjSchema, primaryKeysProj, tag) =
        if (isLeftPrimary(joinType))
          (left, leftProjSchema, leftKeysProj, leftTag)
        else (right, rightProjSchema, rightKeysProj, rightTag)
      val (foreign, foreignProjSchema) =
        if (isLeftPrimary(joinType))
          (right, rightProjSchema)
        else (left, leftProjSchema)

      val primaryProj = EncryptedProjectExec(primaryProjSchema, planLater(primary))
      val foreignProj = EncryptedProjectExec(foreignProjSchema, planLater(foreign))
      val unioned = EncryptedUnionExec(primaryProj, foreignProj)

      // We partition based on the join keys only, so that rows from both the primary and foreign tables that match
      // will colocate to the same partition.
      val partitionOrder = primaryKeysProj.map(k => SortOrder(k, Ascending))
      val partitioned = EncryptedRangePartitionExec(partitionOrder, unioned)
      val sortOrder = sortForJoin(primaryKeysProj, tag, partitioned.output)

      // Add dummy row for the foreign table if outer join.
      val sorted = joinType match {
        case LeftOuter | RightOuter =>
          EncryptedAddDummyRowExec(
            foreignProjSchema.map(_.toAttribute),
            EncryptedSortExec(sortOrder, false, partitioned)
          )
        case _ =>
          EncryptedSortExec(sortOrder, false, partitioned)
      }

      val joined = EncryptedSortMergeJoinExec(
        joinType,
        leftKeysProj,
        rightKeysProj,
        leftProjSchema.map(_.toAttribute),
        rightProjSchema.map(_.toAttribute),
        condition,
        sorted
      )

      val tagsDropped = joinType match {
        case Inner | LeftOuter | RightOuter =>
          EncryptedProjectExec(dropTags(left.output, right.output), joined)
        case LeftExistence(_) => EncryptedProjectExec(left.output, joined)
      }

      tagsDropped :: Nil

    // Used to match non-equi joins
    case Join(left, right, joinType, condition, hint)
        if isEncrypted(left) && isEncrypted(right) =>
      // How to pick broadcast side: if left join, broadcast right. If right join, broadcast left.
      // This is the simplest and most performant method, but may be worth revisting if one side is
      // significantly smaller than the other. Otherwise, pick the smallest side to broadcast.
      // NOTE: the current implementation of BNLJ only works under the assumption that
      // left join <==> broadcast right AND right join <==> broadcast left.
      val desiredBuildSide =
        if (joinType.isInstanceOf[InnerLike] || joinType == FullOuter)
          getSmallerSide(left, right)
        else
          getBroadcastSideBNLJ((joinType))

      val joined = EncryptedBroadcastNestedLoopJoinExec(
        planLater(left),
        planLater(right),
        desiredBuildSide,
        joinType,
        condition
      )

      joined :: Nil

    case a @ PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
        if (isEncrypted(child) && aggExpressions
          .forall(expr => expr.isInstanceOf[AggregateExpression])) =>
      val aggregateExpressions =
        aggExpressions.map(expr => expr.asInstanceOf[AggregateExpression])
      val (functionsWithDistinct, functionsWithoutDistinct) =
        aggregateExpressions.partition(_.isDistinct)

      functionsWithDistinct.size match {
        case 0 => // No distinct aggregate operations
          if (groupingExpressions.size == 0) {
            // Global aggregation
            val partialAggregate = EncryptedAggregateExec(
              groupingExpressions,
              aggregateExpressions.map(_.copy(mode = Partial)),
              planLater(child)
            )
            val partialOutput = partialAggregate.output
            val (projSchema, tag) = tagForGlobalAggregate(partialOutput)

            EncryptedProjectExec(
              resultExpressions,
              EncryptedAggregateExec(
                groupingExpressions,
                aggregateExpressions.map(_.copy(mode = Final)),
                EncryptedProjectExec(
                  partialOutput,
                  EncryptedSortExec(
                    Seq(SortOrder(tag, Ascending)),
                    true,
                    EncryptedProjectExec(projSchema, partialAggregate)
                  )
                )
              )
            ) :: Nil
          } else {
            // Grouping aggregation
            EncryptedProjectExec(
              resultExpressions,
              EncryptedAggregateExec(
                groupingExpressions,
                aggregateExpressions.map(_.copy(mode = Final)),
                EncryptedSortExec(
                  groupingExpressions.map(_.toAttribute).map(e => SortOrder(e, Ascending)),
                  true,
                  EncryptedAggregateExec(
                    groupingExpressions,
                    aggregateExpressions.map(_.copy(mode = Partial)),
                    EncryptedSortExec(
                      groupingExpressions.map(e => SortOrder(e, Ascending)),
                      false,
                      planLater(child)
                    )
                  )
                )
              )
            ) :: Nil
          }
        case size if size == 1 => // One distinct aggregate operation
          // Because we are also grouping on the columns used in the distinct expressions,
          // we do not need separate cases for global and grouping aggregation.

          // We need to extract named expressions from the children of the distinct aggregate functions
          // in order to group by those columns.
          val namedDistinctExpressions =
            functionsWithDistinct.head.aggregateFunction.children.flatMap { e =>
              e match {
                case ne: NamedExpression =>
                  Seq(ne)
                case _ =>
                  e.children
                    .filter(child => child.isInstanceOf[NamedExpression])
                    .map(child => child.asInstanceOf[NamedExpression])
              }
            }
          val combinedGroupingExpressions = groupingExpressions ++ namedDistinctExpressions

          // 1. Create an Aggregate operator for partial aggregations.
          val partialAggregate = {
            val sorted = EncryptedSortExec(
              combinedGroupingExpressions.map(e => SortOrder(e, Ascending)),
              false,
              planLater(child)
            )
            EncryptedAggregateExec(
              combinedGroupingExpressions,
              functionsWithoutDistinct.map(_.copy(mode = Partial)),
              sorted
            )
          }

          // 2. Create an Aggregate operator for partial merge aggregations.
          val partialMergeAggregate = {
            // Partition based on the final grouping expressions.
            val partitionOrder = groupingExpressions.map(e => SortOrder(e, Ascending))
            val partitioned = EncryptedRangePartitionExec(partitionOrder, partialAggregate)

            // Local sort on the combined grouping expressions.
            val sortOrder = combinedGroupingExpressions.map(e => SortOrder(e, Ascending))
            val sorted = EncryptedSortExec(sortOrder, false, partitioned)

            EncryptedAggregateExec(
              combinedGroupingExpressions,
              functionsWithoutDistinct.map(_.copy(mode = PartialMerge)),
              sorted
            )
          }

          // 3. Create an Aggregate operator for partial aggregation of distinct aggregate expressions.
          val partialDistinctAggregate = {
            // Indistinct functions operate on aggregation buffers since partial aggregation was already called,
            // but distinct functions operate on the original input to the aggregation.
            EncryptedAggregateExec(
              groupingExpressions,
              functionsWithoutDistinct.map(_.copy(mode = PartialMerge)) ++
                functionsWithDistinct.map(_.copy(mode = Partial)),
              partialMergeAggregate
            )
          }

          // 4. Create an Aggregate operator for the final aggregation.
          val finalAggregate = {
            val sorted = EncryptedSortExec(
              groupingExpressions.map(e => SortOrder(e, Ascending)),
              true,
              partialDistinctAggregate
            )
            EncryptedAggregateExec(
              groupingExpressions,
              (functionsWithoutDistinct ++ functionsWithDistinct).map(_.copy(mode = Final)),
              sorted
            )
          }

          EncryptedProjectExec(resultExpressions, finalAggregate) :: Nil

        case _ => { // More than one distinct operations
          throw new UnsupportedOperationException(
            "Aggregate operations with more than one distinct expressions are not yet supported."
          )
        }
      }

    case p @ Union(Seq(left, right), _, _) if isEncrypted(p) =>
      EncryptedUnionExec(planLater(left), planLater(right)) :: Nil

    case ReturnAnswer(rootPlan) =>
      rootPlan match {
        case Limit(IntegerLiteral(limit), Sort(sortExprs, true, child)) if isEncrypted(child) =>
          EncryptedGlobalLimitExec(
            limit,
            EncryptedLocalLimitExec(limit, EncryptedSortExec(sortExprs, true, planLater(child)))
          ) :: Nil

        case Limit(IntegerLiteral(limit), Project(projectList, child)) if isEncrypted(child) =>
          EncryptedGlobalLimitExec(
            limit,
            EncryptedLocalLimitExec(limit, EncryptedProjectExec(projectList, planLater(child)))
          ) :: Nil

        case _ => Nil
      }

    case Limit(IntegerLiteral(limit), Sort(sortExprs, true, child)) if isEncrypted(child) =>
      EncryptedGlobalLimitExec(
        limit,
        EncryptedLocalLimitExec(limit, EncryptedSortExec(sortExprs, true, planLater(child)))
      ) :: Nil

    case Limit(IntegerLiteral(limit), Project(projectList, child)) if isEncrypted(child) =>
      EncryptedGlobalLimitExec(
        limit,
        EncryptedLocalLimitExec(limit, EncryptedProjectExec(projectList, planLater(child)))
      ) :: Nil

    case LocalLimit(IntegerLiteral(limit), child) if isEncrypted(child) =>
      EncryptedLocalLimitExec(limit, planLater(child)) :: Nil

    case GlobalLimit(IntegerLiteral(limit), child) if isEncrypted(child) =>
      EncryptedGlobalLimitExec(limit, planLater(child)) :: Nil

    case Encrypt(child) =>
      EncryptExec(planLater(child)) :: Nil

    case EncryptedLocalRelation(output, plaintextData) =>
      EncryptedLocalTableScanExec(output, plaintextData) :: Nil

    case EncryptedBlockRDD(output, rdd) =>
      EncryptedBlockRDDScanExec(output, rdd) :: Nil

    case _ =>
      if (isEncrypted(plan)) {
        throw new OpaqueException(
          s"Logical operator ${plan.nodeName}(${plan.argString(10)}) is currently not supported in Opaque"
        )
      }

      Nil
  }

  private def tagForEquiJoin(
      keys: Seq[Expression],
      input: Seq[Attribute],
      isPrimary: Boolean
  ): (Seq[NamedExpression], Seq[NamedExpression], NamedExpression) = {
    val keysProj = keys.zipWithIndex.map { case (k, i) => Alias(k, "_" + i)() }
    val tag = Alias(Literal(if (isPrimary) 0 else 1), "_tag")()
    (Seq(tag) ++ keysProj ++ input, keysProj.map(_.toAttribute), tag.toAttribute)
  }

  private def dropTags(
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute]
  ): Seq[NamedExpression] =
    leftOutput ++ rightOutput

  private def sortForJoin(
      leftKeys: Seq[Expression],
      tag: Expression,
      input: Seq[Attribute]
  ): Seq[SortOrder] =
    leftKeys.map(k => SortOrder(k, Ascending)) :+ SortOrder(tag, Ascending)

  private def tagForGlobalAggregate(
      input: Seq[Attribute]
  ): (Seq[NamedExpression], NamedExpression) = {
    val tag = Alias(Literal(0), "_tag")()
    (Seq(tag) ++ input, tag.toAttribute)
  }

  private def isLeftPrimary(joinType: JoinType): Boolean = joinType match {
    case RightOuter => false
    case _ => true
  }

  private def getBroadcastSideBNLJ(joinType: JoinType): BuildSide = joinType match {
    case LeftExistence(_) | LeftOuter => BuildRight
    case _ => BuildLeft
  }
}
