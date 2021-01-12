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

package org.apache.flink.formats.avro.registry.confluent.debezium;

import org.apache.avro.generic.GenericRecord;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentSchemaRegistryCoder;
import org.apache.flink.formats.avro.registry.confluent.debezium.DebeziumAvroDecodingFormat.ReadableMetadata;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Deserialization schema from Debezium Avro to Flink Table/SQL internal data structure {@link
 * RowData}. The deserialization schema knows Debezium's schema definition and can extract the
 * database data and convert into {@link RowData} with {@link RowKind}. Deserializes a <code>byte[]
 * </code> message as a JSON object and reads the specified fields. Failures during deserialization
 * are forwarded as wrapped IOExceptions.
 *
 * @see <a href="https://debezium.io/">Debezium</a>
 */
@Internal
public final class DebeziumAvroDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /** snapshot read. */
    private static final String OP_READ = "r";
    /** insert operation. */
    private static final String OP_CREATE = "c";
    /** update operation. */
    private static final String OP_UPDATE = "u";
    /** delete operation. */
    private static final String OP_DELETE = "d";

    private static final String REPLICA_IDENTITY_EXCEPTION =
            "The \"before\" field of %s message is null, "
                    + "if you are using Debezium Postgres Connector, "
                    + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.";

    /** The deserializer to deserialize Debezium Avro data. */
    private final AvroRowDataDeserializationSchema avroDeserializer;

    /** TypeInformation of the produced {@link RowData}. */
    private final TypeInformation<RowData> producedTypeInfo;

    /** Flag that indicates that an additional projection is required for metadata. */
    private final boolean hasMetadata;

    /** Metadata to be extracted for every record. */
    private final MetadataConverter[] metadataConverters;

    public DebeziumAvroDeserializationSchema(
            DataType physicalDataType,
            TypeInformation<RowData> producedTypeInfo,
            List<ReadableMetadata> requestedMetadata,
            String schemaRegistryUrl) {
        this.producedTypeInfo = producedTypeInfo;
        this.hasMetadata = requestedMetadata.size() > 0;
        final RowType debeziumAvroRowType =
                createDebeziumAvroRowType(physicalDataType, requestedMetadata);
        this.avroDeserializer =
                new AvroRowDataDeserializationSchema(
                        ConfluentRegistryAvroDeserializationSchema.forGeneric(
                                AvroSchemaConverter.convertToSchema(debeziumAvroRowType),
                                schemaRegistryUrl),
                        AvroToRowDataConverters.createRowConverter(debeziumAvroRowType),
                        producedTypeInfo);
        this.metadataConverters =
                createMetadataConverters(debeziumAvroRowType, requestedMetadata);
    }

    @VisibleForTesting
    public DebeziumAvroDeserializationSchema(
            DataType physicalDataType,
            TypeInformation<RowData> producedTypeInfo,
            List<ReadableMetadata> requestedMetadata,
            ConfluentSchemaRegistryCoder registryCoder) {
        this.producedTypeInfo = producedTypeInfo;
        this.hasMetadata = requestedMetadata.size() > 0;
        final RowType debeziumAvroRowType =
                createDebeziumAvroRowType(physicalDataType, requestedMetadata);
        this.avroDeserializer =
                new AvroRowDataDeserializationSchema(
                        new RegistryAvroDeserializationSchema<>(
                                GenericRecord.class,
                                AvroSchemaConverter.convertToSchema(debeziumAvroRowType),
                                () -> registryCoder),
                        AvroToRowDataConverters.createRowConverter(debeziumAvroRowType),
                        producedTypeInfo);
        this.metadataConverters =
                createMetadataConverters(debeziumAvroRowType, requestedMetadata);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        avroDeserializer.open(context);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {

        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }
        try {
            GenericRowData row = (GenericRowData) avroDeserializer.deserialize(message);

            GenericRowData before = (GenericRowData) row.getField(0);
            GenericRowData after = (GenericRowData) row.getField(1);
            String op = row.getField(2).toString();
            if (OP_CREATE.equals(op) || OP_READ.equals(op)) {
                after.setRowKind(RowKind.INSERT);
                emitRow(row, after, out);
            } else if (OP_UPDATE.equals(op)) {
                if (before == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
                }
                before.setRowKind(RowKind.UPDATE_BEFORE);
                after.setRowKind(RowKind.UPDATE_AFTER);
                emitRow(row, before, out);
                emitRow(row, after, out);
            } else if (OP_DELETE.equals(op)) {
                if (before == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
                }
                before.setRowKind(RowKind.DELETE);
                emitRow(row, before, out);
            } else {
                throw new IOException(
                        format(
                                "Unknown \"op\" value \"%s\". The Debezium Avro message is '%s'",
                                op, new String(message)));
            }
        } catch (Throwable t) {
            // a big try catch to protect the processing.
            throw new IOException("Can't deserialize Debezium Avro message.", t);
        }
    }

    private void emitRow(
            GenericRowData rootRow, GenericRowData physicalRow, Collector<RowData> out) {
        // shortcut in case no output projection is required
        if (!hasMetadata) {
            out.collect(physicalRow);
            return;
        }

        final int physicalArity = physicalRow.getArity();
        final int metadataArity = metadataConverters.length;
        final GenericRowData producedRow =
                new GenericRowData(physicalRow.getRowKind(), physicalArity + metadataArity);
        for (int physicalPos = 0; physicalPos < physicalArity; physicalPos++) {
            producedRow.setField(physicalPos, physicalRow.getField(physicalPos));
        }
        for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
            producedRow.setField(
                    physicalArity + metadataPos, metadataConverters[metadataPos].convert(rootRow));
        }

        out.collect(producedRow);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DebeziumAvroDeserializationSchema that = (DebeziumAvroDeserializationSchema) o;
        return Objects.equals(avroDeserializer, that.avroDeserializer)
                && Objects.equals(producedTypeInfo, that.producedTypeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(avroDeserializer, producedTypeInfo);
    }

    public static RowType createDebeziumAvroRowType(
            DataType physicalDataType,
            List<ReadableMetadata> readableMetadata) {

        final List<DataTypes.Field> sourceMetadataFields =
                readableMetadata.stream()
                        .filter(m -> m.isSourceField)
//                        .map(m -> DataTypes.FIELD(m.key, m.dataType))
                        .map(m -> m.requiredField)
//                        .distinct()
                        .collect(Collectors.toList());

        DataType root =
                DataTypes.ROW(
                        DataTypes.FIELD("before", physicalDataType),
                        DataTypes.FIELD("after", physicalDataType),
                        DataTypes.FIELD("op", DataTypes.STRING()));
        // append fields that are required for reading metadata
        final List<DataTypes.Field> rootMetadataFields =
                readableMetadata.stream()
                        .filter(m -> !m.isSourceField)
//                        .map(m -> DataTypes.FIELD(m.key, m.dataType))
                        .map(m -> m.requiredField)
//                        .distinct()
                        .collect(Collectors.toList());

        root = DataTypeUtils.appendRowFields(root, rootMetadataFields);



        DataType sourceElements = DataTypes.ROW(sourceMetadataFields.toArray(new DataTypes.Field[0]));


        return (RowType) DataTypeUtils.appendRowFields(root,
                Collections.singletonList(DataTypes.FIELD("source", sourceElements))).getLogicalType();
    }

    private static MetadataConverter[] createMetadataConverters(
            RowType rowType, List<ReadableMetadata> requestedMetadata) {
        return requestedMetadata.stream()
                .map(
                        m -> {
                            if (!m.isSourceField) {
                                return convertInRoot(rowType, m);
                            } else {
                                return convertInSource(rowType, m);
                            }
                        })
                .toArray(MetadataConverter[]::new);
    }

    private static MetadataConverter convertInRoot(RowType rowType, ReadableMetadata metadata) {
        final int pos = findFieldPos(metadata, rowType);
        return new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(GenericRowData root, int unused) {
                return metadata.converter.convert(root, pos);
            }
        };
    }

    private static MetadataConverter convertInSource(
            RowType rowType, ReadableMetadata metadata) {

            final int pos = findFieldPos(metadata, (RowType) rowType.getChildren().get(rowType.getFieldCount() - 1));
            return new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object convert(GenericRowData root, int unused) {
                    final GenericRowData payload = (GenericRowData) root.getField(rowType.getFieldCount() - 1);
                    return metadata.converter.convert(payload, pos);
                }
            };
    }

    private static int findFieldPos(ReadableMetadata metadata, RowType rowType) {
        return rowType.getFieldNames().indexOf(metadata.requiredField.getName());
    }

    /**
     * Converter that extracts a metadata field from the row that comes out of the
     * AVRO schema and converts it to the desired data type.
     */
    interface MetadataConverter extends Serializable {

        // Method for top-level access.
        default Object convert(GenericRowData row) {
            return convert(row, -1);
        }

        Object convert(GenericRowData row, int pos);
    }

}
