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

package edu.berkeley.cs.rise.opaque.execution

import scala.collection.mutable.ArrayBuffer

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.JobVerificationEngine
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.TaskContext

trait LeafExecNode extends SparkPlan {
  override final def children: Seq[SparkPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

trait UnaryExecNode extends SparkPlan {
  def child: SparkPlan

  override final def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

trait BinaryExecNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override final def children: Seq[SparkPlan] = Seq(left, right)
}

case class EncryptedLocalTableScanExec(
    output: Seq[Attribute],
    plaintextData: Seq[InternalRow])
  extends LeafExecNode with OpaqueOperatorExec {

  private val unsafeRows: Array[InternalRow] = {
    val proj = UnsafeProjection.create(output, output)
    val result: Array[InternalRow] = plaintextData.map(r => proj(r).copy()).toArray
    result
  }

  override def executeBlocked(): RDD[Block] = {
    // Locally partition plaintextData using the same logic as ParallelCollectionRDD.slice
    println("Scala Operator: EncryptedLocalTableScanExec")
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
    val slicedPlaintextData: Seq[Seq[InternalRow]] =
      positions(unsafeRows.length, sqlContext.sparkContext.defaultParallelism).map {
        case (start, end) => unsafeRows.slice(start, end).toSeq
      }.toSeq

    // Encrypt each local partition
    val encryptedPartitions: Seq[Block] =
      slicedPlaintextData.map(slice =>
        Utils.encryptInternalRowsFlatbuffers(slice, output.map(_.dataType), useEnclave = false))

    // Make an RDD from the encrypted partitions
    sqlContext.sparkContext.parallelize(encryptedPartitions)
  }
}

case class EncryptExec(child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): RDD[Block] = {
    println("Scala Operator: EncryptExec")
    child.execute().mapPartitions { rowIter =>
      Iterator(Utils.encryptInternalRowsFlatbuffers(
        rowIter.toSeq, output.map(_.dataType), useEnclave = true))
    }
  }
}

case class EncryptedBlockRDDScanExec(
    output: Seq[Attribute],
    rdd: RDD[Block])
  extends LeafExecNode with OpaqueOperatorExec {

  override def executeBlocked(): RDD[Block] = rdd
}

case class Block(bytes: Array[Byte]) extends Serializable

trait OpaqueOperatorExec extends SparkPlan {
  def executeBlocked(): RDD[Block]

  def timeOperator[A](childRDD: RDD[A], desc: String)(f: RDD[A] => RDD[Block]): RDD[Block] = {
    import Utils.time
    Utils.ensureCached(childRDD)
    time(s"Force child of $desc") { childRDD.count }
    time(desc) {
      val result = f(childRDD)
      Utils.ensureCached(result)
      result.count
      result
    }
  }

  /**
   * An Opaque operator cannot return plaintext rows, so this method should normally not be invoked.
   * Instead use executeBlocked, which returns the data as encrypted blocks.
   *
   * However, when encrypted data is cached, Spark SQL's InMemoryRelation attempts to call this
   * method and persist the resulting RDD. [[ConvertToOpaqueOperators]] later eliminates the dummy
   * relation from the logical plan, but this only happens after InMemoryRelation has called this
   * method. We therefore have to silently return an empty RDD here.
   */
  override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.emptyRDD
    // throw new UnsupportedOperationException("use executeBlocked")
  }

  override def executeCollect(): Array[InternalRow] = {
    println("Scala Operator: Collect")

    val collectedRDD = executeBlocked().collect()
    collectedRDD.map { block =>
        Utils.addBlockForVerification(block)
    }

    val postVerificationPasses = Utils.verifyJob()
    JobVerificationEngine.resetForNextJob()
    // val postVerificationPasses = true
    if (postVerificationPasses) {
      collectedRDD.flatMap { block =>
        Utils.decryptBlockFlatbuffers(block)
      }
    } else {
      throw new Exception("Post Verification Failed")
    }
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    println("executeTake called")
    if (n == 0) {
      return new Array[InternalRow](0)
    }

    val childRDD = executeBlocked()

    val buf = new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * n * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val sc = sqlContext.sparkContext
      val res = sc.runJob(childRDD,
        (it: Iterator[Block]) => if (it.hasNext) Some(it.next()) else None, p)

      res.foreach {
        case Some(block) =>
          buf ++= Utils.decryptBlockFlatbuffers(block)
        case None =>
      }

      partsScanned += p.size
    }

    if (buf.size > n) {
      buf.take(n).toArray
    } else {
      buf.toArray
    }
  }
}

case class EncryptedProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def executeBlocked(): RDD[Block] = {
    println("Scala Operator: Encrypted Project Exec")
    val projectListSer = Utils.serializeProjectList(projectList, child.output)
    timeOperator(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), "EncryptedProjectExec") {
      childRDD => 
        JobVerificationEngine.addExpectedOperator("EncryptedProjectExec")
        childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.Project(eid, projectListSer, block.bytes, TaskContext.getPartitionId))
      }
    }
  }
}


case class EncryptedFilterExec(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): RDD[Block] = {
    println("Scala Operator: Encrypted Filter Exec")
    val conditionSer = Utils.serializeFilterExpression(condition, child.output)
    timeOperator(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), "EncryptedFilterExec") {
      childRDD => 
        JobVerificationEngine.addExpectedOperator("EncryptedFilterExec")
        childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.Filter(eid, conditionSer, block.bytes, TaskContext.getPartitionId))
      }
    }
  }
}

case class EncryptedAggregateExec(
    groupingExpressions: Seq[Expression],
    aggExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def producedAttributes: AttributeSet =
    AttributeSet(aggExpressions) -- AttributeSet(groupingExpressions)

  override def output: Seq[Attribute] = aggExpressions.map(_.toAttribute)

  override def executeBlocked(): RDD[Block] = {
    println("Scala Operator: encrypted Aggregate exec")
    val aggExprSer = Utils.serializeAggOp(groupingExpressions, aggExpressions, child.output)

    timeOperator(
      child.asInstanceOf[OpaqueOperatorExec].executeBlocked(),
      "EncryptedAggregateExec") { childRDD =>

      JobVerificationEngine.addExpectedOperator("EncryptedAggregateExec")
      val (firstRows, lastGroups, lastRows) = childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        val (firstRow, lastGroup, lastRow) = enclave.NonObliviousAggregateStep1(
          eid, aggExprSer, block.bytes, TaskContext.getPartitionId)
        (Block(firstRow), Block(lastGroup), Block(lastRow))
      }.collect.unzip3

      // Send first row to previous partition and last group to next partition
      var shiftedFirstRows = Array[Block]()
      var shiftedLastGroups = Array[Block]()
      var shiftedLastRows = Array[Block]()
      if (childRDD.getNumPartitions == 1) {
        val firstRowDrop = firstRows(0)
        shiftedFirstRows = firstRows.drop(1) :+ Utils.emptyBlock(firstRowDrop)

        val lastGroupDrop = lastGroups.last
        shiftedLastGroups = Utils.emptyBlock(lastGroupDrop) +: lastGroups.dropRight(1)

        val lastRowDrop = lastRows.last
        shiftedLastRows = Utils.emptyBlock(lastRowDrop) +: lastRows.dropRight(1)
      } else {
        shiftedFirstRows = firstRows.drop(1) :+ Utils.emptyBlock
        shiftedLastGroups = Utils.emptyBlock +: lastGroups.dropRight(1)
        shiftedLastRows = Utils.emptyBlock +: lastRows.dropRight(1)
      }

      val shifted = (shiftedFirstRows, shiftedLastGroups, shiftedLastRows).zipped.toSeq
      assert(shifted.size == childRDD.partitions.length)
      val shiftedRDD = sparkContext.parallelize(shifted, childRDD.partitions.length)

      childRDD.zipPartitions(shiftedRDD) { (blockIter, boundaryIter) =>
        (blockIter.toSeq, boundaryIter.toSeq) match {
          case (Seq(block), Seq(Tuple3(
            nextPartitionFirstRow, prevPartitionLastGroup, prevPartitionLastRow))) =>
            val (enclave, eid) = Utils.initEnclave()
            Iterator(Block(enclave.NonObliviousAggregateStep2(
              eid, aggExprSer, block.bytes,
              nextPartitionFirstRow.bytes, prevPartitionLastGroup.bytes,
              prevPartitionLastRow.bytes, TaskContext.getPartitionId)))
        }
      }
    }
  }
}

case class EncryptedSortMergeJoinExec(
    joinType: JoinType,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    leftSchema: Seq[Attribute],
    rightSchema: Seq[Attribute],
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def executeBlocked(): RDD[Block] = {
    println("Scala Operator: Encrypted Sort Merge Join Exec")
    val joinExprSer = Utils.serializeJoinExpression(
      joinType, leftKeys, rightKeys, leftSchema, rightSchema)

    timeOperator(
      child.asInstanceOf[OpaqueOperatorExec].executeBlocked(),
      "EncryptedSortMergeJoinExec") { childRDD =>

      JobVerificationEngine.addExpectedOperator("EncryptedSortMergeJoinExec")
      val lastPrimaryRows = childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.ScanCollectLastPrimary(eid, joinExprSer, block.bytes, TaskContext.getPartitionId))
      }.collect

      var shifted = Array[Block]()
      if (childRDD.getNumPartitions == 1) {
        val lastLastPrimaryRow = lastPrimaryRows.last
        shifted = Utils.emptyBlock(lastLastPrimaryRow) +: lastPrimaryRows.dropRight(1)
      } else {
        shifted = Utils.emptyBlock +: lastPrimaryRows.dropRight(1)
      }
      assert(shifted.size == childRDD.partitions.length)
      val processedJoinRowsRDD =
        sparkContext.parallelize(shifted, childRDD.partitions.length)

      childRDD.zipPartitions(processedJoinRowsRDD) { (blockIter, joinRowIter) =>
        (blockIter.toSeq, joinRowIter.toSeq) match {
          case (Seq(block), Seq(joinRow)) =>
            val (enclave, eid) = Utils.initEnclave()
            Iterator(Block(enclave.NonObliviousSortMergeJoin(
              eid, joinExprSer, block.bytes, joinRow.bytes, TaskContext.getPartitionId)))
        }
      }
    }
  }
}

case class EncryptedUnionExec(
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryExecNode with OpaqueOperatorExec {
  import Utils.time

  override def output: Seq[Attribute] =
    left.output

  override def executeBlocked(): RDD[Block] = {
    println("Scala Operator: encrypted union exec")
    var leftRDD = left.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    var rightRDD = right.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    Utils.ensureCached(leftRDD)
    time("Force left child of EncryptedUnionExec") { leftRDD.count }
    Utils.ensureCached(rightRDD)
    time("Force right child of EncryptedUnionExec") { rightRDD.count }

    // RA.initRA(leftRDD)

    val num_left_partitions = leftRDD.getNumPartitions
    val num_right_partitions = rightRDD.getNumPartitions
    if (num_left_partitions != num_right_partitions) {
      if (num_left_partitions > num_right_partitions) {
        leftRDD = leftRDD.coalesce(num_right_partitions)
      } else {
        rightRDD = rightRDD.coalesce(num_left_partitions)
      }
    }
    val unioned = leftRDD.zipPartitions(rightRDD) {
      (leftBlockIter, rightBlockIter) =>
        Iterator(Utils.concatEncryptedBlocks(leftBlockIter.toSeq ++ rightBlockIter.toSeq))
    }
    Utils.ensureCached(unioned)
    time("EncryptedUnionExec") { unioned.count }
    unioned
  }
}
