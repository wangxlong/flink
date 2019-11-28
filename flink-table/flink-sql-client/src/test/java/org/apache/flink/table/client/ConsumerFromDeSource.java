package org.apache.flink.table.client;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;


public class ConsumerFromDeSource {

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
					Thread.sleep(1000 * 10);
				}
			}
			@Override
			public void cancel() {

			}
		});

		// create and register a TableSink
		String tableCsvSinkDDL = "create table RubberOrders(\n" +
			"                        a DOUBLE,\n" +
			"                        b varchar,\n" +
			"                        c int\n" +
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
			"  'update-mode' = 'append' " +
			" "+
			""+
			"                      )";


		tableEnv.registerDataStream("Orders", ds, "user, currency, amount");

		String[] fieldNames = {"a", "b", "c"};
		TypeInformation[] fieldTypes = {Types.DOUBLE, Types.STRING, Types.INT};
		tableEnv.registerTableSink("aaaa", new MyRetractSink(fieldNames, fieldTypes));


		tableEnv.sqlUpdate(tableCsvSinkDDL);

//			String sql = "INSERT INTO aaaa SELECT * FROM Orders AS o left join Rate AS r on r.currency = o.currency and o.amount = 2 ";
//		tableEnv.sqlUpdate(sql);

		String sql = "INSERT INTO RubberOrders SELECT user, currency, bitand(1, 1) FROM Orders ";
		tableEnv.sqlUpdate(sql);

		bsEnv.execute("m");
	}

}
