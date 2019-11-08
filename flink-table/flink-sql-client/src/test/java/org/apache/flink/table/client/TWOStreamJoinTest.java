package org.apache.flink.table.client;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;


public class TWOStreamJoinTest {

	public static void main(String [] args) throws Exception {

		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

		// ingest a DataStream from an external source
		DataStream<Tuple3<Double, String, Integer>> ds = bsEnv.addSource(new SourceFunction<Tuple3<Double, String, Integer>>() {
			@Override
			public void run(SourceContext<Tuple3<Double, String, Integer>> sourceContext) throws Exception {
				int i = 0;
				while (true) {
					sourceContext.collect(new Tuple3<>(1.0, "Yen2", i++));
					Thread.sleep(1000 * 1);
				}
			}
			@Override
			public void cancel() {

			}
		});

		tableEnv.registerDataStream("Orders", ds, "user, currency, amount");

		DataStream<Tuple2<String, Long>> ratesHistoryStream = bsEnv.addSource(new SourceFunction<Tuple2<String, Long>>() {
			@Override
			public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
				String name = "Yen";
				String temp = "Yen";
				long i = 0;
				while (true) {
					ctx.collect( new Tuple2(temp, i++));
					Thread.sleep(1000 * 3);
					temp = name + i;
				}
			}
			@Override
			public void cancel() {

			}
		});
		tableEnv.registerDataStream("Rate", ratesHistoryStream, "currency, rate");

		// create and register a TableSink
		String tableCsvSinkDDL = "create table RubberOrders(\n" +
			"                        a DOUBLE,\n" +
			"                        b varchar,\n" +
			"                        c int,\n" +
			"                        d varchar,\n" +
			"                        e bigint\n" +
			"                      ) with (\n" +
			"                        'connector.type' = 'filesystem',\n" +
			"                        'format.type' = 'csv',\n" +
			"                        'connector.path' = '/Users/didi/Desktop/ddl/qs',\n" +
			"						 'format.fields.0.type' = 'DOUBLE'," +
			" 'connector.property-version' = '1', "+
			" 'format.fields.0.name' = 'a'," +
			" 'format.fields.1.type' = 'STRING'," +
			" 'format.fields.1.name' = 'b'," +
			" 'format.fields.2.type' = 'INT'," +
			" 'format.fields.2.name' = 'c'," +
			" 'format.fields.3.type' = 'STRING'," +
			" 'format.fields.3.name' = 'd'," +
			" 'format.fields.4.type' = 'LONG'," +
			" 'format.fields.4.name' = 'e'" +
			" "+
			""+
			"                      )";



		String tableMysqlSink = "create table fiveFields(\n" +
			"                       a DOUBLE,\n" +
			"                        b varchar,\n" +
			"                        c int,\n" +
			"                        d varchar,\n" +
			"                        e bigint\n" +
			"                      ) with (\n" +
			"                        'connector.type' = 'jdbc',\n" +
			" 'connector.url' = 'jdbc:mysql://localhost:3306/whl?serverTimezone=GMT'," +
			" 'connector.table' = 'fiveFields'," +
			" 'connector.username' = 'root'," +
			" 'connector.driver' = 'com.mysql.jdbc.Driver'," +
			" 'connector.write.flush.max-rows' = '1'," +
			" 'connector.password' = 'wanglin000'" +
			")";


		String[] fieldNames = {"a", "b", "c", "d", "e"};
		TypeInformation[] fieldTypes = {Types.DOUBLE, Types.STRING, Types.INT, Types.STRING, Types.LONG};
		tableEnv.registerTableSink("aaaa", new MyRetractSink(fieldNames, fieldTypes));

		tableEnv.sqlUpdate(tableMysqlSink);

			String sql = "INSERT INTO aaaa SELECT * FROM Orders AS o left join Rate AS r on r.currency = o.currency and o.amount = 2 ";
		tableEnv.sqlUpdate(sql);

		bsEnv.execute("m");
	}

}

class MyRetractSink implements RetractStreamTableSink<Row> {


	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;

	public MyRetractSink(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return  Types.ROW_NAMED(fieldNames, fieldTypes);
	}

	@Override
	public void emitDataStream(DataStream dataStream) {
		dataStream.print();
	}

	@Override
	public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
		MyRetractSink myRetractSink = new MyRetractSink(fieldNames, fieldTypes);
		myRetractSink.fieldNames = fieldNames;
		myRetractSink.fieldTypes = fieldTypes;
		return myRetractSink;
	}


	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		return dataStream.print();
	}
}
