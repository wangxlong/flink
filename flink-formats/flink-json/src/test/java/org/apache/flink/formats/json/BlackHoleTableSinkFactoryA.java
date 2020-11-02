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

package org.apache.flink.formats.json;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * Black hole table sink factory swallowing all input records. It is designed for:
 * - high performance testing.
 * - UDF to output, not substantive sink.
 * Just like /dev/null device on Unix-like operating systems.
 */
@PublicEvolving
public class BlackHoleTableSinkFactoryA implements DynamicTableSinkFactory {

	public static final String IDENTIFIER = "Myblackhole";

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return new HashSet<>();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return new HashSet<>();
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		createTableFactoryHelper(this, context).validate();

		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
			SerializationFormatFactory.class,
			FactoryUtil.FORMAT);

		final DataType physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

		return new BlackHoleSink(encodingFormat, physicalDataType);
	}

	private static class BlackHoleSink implements DynamicTableSink {

		private transient EncodingFormat<SerializationSchema<RowData>> encodingFormat;
		DataType physicalDataType;

		public BlackHoleSink(
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			DataType physicalDataType){
			this.encodingFormat = encodingFormat;
			this.physicalDataType = physicalDataType;
		}

		@Override
		public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
			ChangelogMode.Builder builder = ChangelogMode.newBuilder();
			for (RowKind kind : requestedMode.getContainedKinds()) {
				if (kind != RowKind.UPDATE_BEFORE) {
					builder.addContainedKind(kind);
				}
			}
			return builder.build();
		}

		@Override
		public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
			final SerializationSchema<RowData> valueSerialization =
				this.encodingFormat.createRuntimeEncoder(context, this.physicalDataType);
			return SinkFunctionProvider.of(new DiscardingSink<RowData>(valueSerialization));
		}

		@Override
		public DynamicTableSink copy() {
			return new BlackHoleSink(encodingFormat, physicalDataType);
		}

		@Override
		public String asSummaryString() {
			return "BlackHole";
		}
	}
}
