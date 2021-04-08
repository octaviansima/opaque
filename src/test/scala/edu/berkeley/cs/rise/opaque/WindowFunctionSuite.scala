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

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

trait WindowFunctionSuite extends OpaqueSuiteBase with SQLHelper {
  import spark.implicits._

  test("reuse window partitionBy") {
    checkAnswer() { sl =>
      val simpleData = Seq(
        ("James", "Sales", 3000),
        ("Michael", "Sales", 4600),
        ("Robert", "Sales", 4100),
        ("Maria", "Finance", 3000),
        ("James", "Sales", 3000),
        ("Scott", "Finance", 3300),
        ("Jen", "Finance", 3900),
        ("Jeff", "Marketing", 3000),
        ("Kumar", "Marketing", 2000),
        ("Saif", "Sales", 4100)
      )
      val df = sl.applyTo(simpleData.toDF("employee_name", "department", "salary"))
      val w = Window.partitionBy("department").orderBy("salary")
      val res = df.withColumn("rowNumber", row_number.over(w))
      res.explain
      res.show
      res
    }
  }

  test("rollup") {
    checkAnswer() { sl =>
      val df =
        sl.applyTo(Seq(("bar", 2L), ("bar", 2L), ("foo", 1L), ("foo", 2L)).toDF("word", "num"))
      val res = df.rollup($"word", $"num").count
      res.explain
      res
    }
  }
}

class SinglePartitionWindowFunctionSuite
    extends WindowFunctionSuite
    with SinglePartitionSparkSession {}

class MultiplePartitionWindowFunctionSuite
    extends WindowFunctionSuite
    with MultiplePartitionSparkSession {}
