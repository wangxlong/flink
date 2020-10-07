/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.expressions.utils

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, PrimitiveArrayTypeInfo, Types}
import org.apache.flink.api.java.typeutils.{MultisetTypeInfo, ObjectArrayTypeInfo, RowTypeInfo}
import org.apache.flink.table.planner.JHashMap
import org.apache.flink.table.planner.utils.DateTimeTestUtil._
import org.apache.flink.types.Row

abstract class MultiSetTypeTestBase extends ExpressionTestBase {

  case class MyCaseClass(string: String, int: Int)

  override def testData: Row = {
    val testData = new Row(2)

    val map = new JHashMap[String, Long]()
    map.put("aaa", 1L)
    val map1 = new JHashMap[Int, Long]()
    map1.put(1, 1L)
    testData.setField(0, map)
    testData.setField(1, map1)
    testData
  }

  override def typeInfo: RowTypeInfo = {
    new RowTypeInfo(
      /* 0 */ new  MultisetTypeInfo(BasicTypeInfo.STRING_TYPE_INFO),
      /* 1 */ new  MultisetTypeInfo(BasicTypeInfo.INT_TYPE_INFO)
    )
  }
}
