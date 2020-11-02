package org.apache.flink.formats.json;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;

public class DiscardingSink<T> implements SinkFunction<T> {

	private transient SerializationSchema<RowData> a;
	private static final long serialVersionUID = 1L;

	public DiscardingSink(SerializationSchema<RowData> a) {
		this.a = a;
	}

	@Override
	public void invoke(T value) {
		a.serialize((RowData) value);
	}
}
