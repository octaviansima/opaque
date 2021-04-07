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
      val df = sl.applyTo(Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value"))
      val w = Window.partitionBy("key").orderBy("value")
      df.select(lead("key", 1).over(w), lead("value", 1).over(w))
    }
  }
}

class SinglePartitionWindowFunctionSuite
    extends WindowFunctionSuite
    with SinglePartitionSparkSession {}

class MultiplePartitionWindowFunctionSuite
    extends WindowFunctionSuite
    with MultiplePartitionSparkSession {}
