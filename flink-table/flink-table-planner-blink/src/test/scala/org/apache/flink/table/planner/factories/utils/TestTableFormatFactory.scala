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

package org.apache.flink.table.planner.factories.utils

import java.util

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.table.factories.TableFormatFactoryBase.deriveSchema
import org.apache.flink.table.factories.{DeserializationSchemaFactory, SerializationSchemaFactory, TableFormatFactoryBase}
import org.apache.flink.types.Row

/**
  * Table format factory for testing.
  *
  * It has the same context as [[TestAmbiguousTableFormatFactory]] and both support COMMON_PATH.
  * This format does not support SPECIAL_PATH but supports schema derivation.
  */
class TestTableFormatFactory
  extends TableFormatFactoryBase[Row]("test-format", 1, true)
  with DeserializationSchemaFactory[Row]
  with SerializationSchemaFactory[Row] {

  val SPECIAL_PATH = "format.special-path"
  val UNIQUE_PROPERTY = "format.unique-property"
  val COMMON_PATH = "format.common-path"

  override def supportedFormatProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()
    properties.add(UNIQUE_PROPERTY)
    properties.add(COMMON_PATH)
    properties
  }

  override def createDeserializationSchema(
      properties: util.Map[String, String])
    : DeserializationSchema[Row] = {

    new TestDeserializationSchema(deriveSchema(properties).toRowType)
  }

  override def createSerializationSchema(
      properties: util.Map[String, String])
    : SerializationSchema[Row] = {

    new TestSerializationSchema(deriveSchema(properties).toRowType)
  }
}
