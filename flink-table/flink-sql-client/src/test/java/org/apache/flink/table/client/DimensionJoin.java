package org.apache.flink.table.client;

import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;

public class DimensionJoin {

	public static void main(String [] args) throws Exception {

		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

		// ingest a DataStream from an external source
		DataStream<Tuple3<Double, String, Integer>> ds = bsEnv.addSource(new SourceFunction<Tuple3<Double, String, Integer>>() {
			@Override
			public void run(SourceContext<Tuple3<Double, String, Integer>> sourceContext) throws Exception {
				int i = 1;
				while (true) {
					sourceContext.collect(new Tuple3<>(1.0, "Rubber", i++));
					Thread.sleep(1000 * 3);
				}
			}
			@Override
			public void cancel() {

			}
		});

		tableEnv.registerDataStream("Orders", ds, "user, currency, amount,proctime.proctime");

		JDBCTableSource.Builder builder = JDBCTableSource.builder()
			.setOptions(JDBCOptions.builder()
				.setDBUrl("jdbc:mysql://localhost:3306/whl?serverTimezone=GMT")
				.setTableName("table2")
				.setUsername("root")
				.setDriverName("com.mysql.jdbc.Driver")
				.setPassword("wanglin000")
				.build())
			.setSchema(TableSchema.builder().fields(
				new String[]{"Id", "amount", "product"},
				new DataType[]{DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()})
				.build());
		JDBCTableSource jd = builder.build();

		tableEnv.registerTableSource("mysqlid", jd);
//
//		Table mysqlid = tableEnv.sqlQuery(
//			"SELECT Id, upper(product)  as product  , amount FROM  resultt");
//		tableEnv.registerTable("mysqlid", mysqlid);

		// create and register a TableSink
		String tableCsvSinkDDL = "create table RubberOrders(\n" +
			"                        a INT,\n" +
			"                        b varchar,\n" +
			"						 c int,\n" +
			"						 d varchar,\n" +
			"						 e varchar\n" +
			"                      ) with (\n" +
			"                        'connector.type' = 'filesystem',\n" +
			"                        'format.type' = 'csv',\n" +
			"                        'connector.path' = '/Users/didi/Desktop/ddl/whl.txt',\n" +
			"						 'format.fields.0.type' = 'INT'," +
			" 'connector.property-version' = '1', "+
			"'format.fields.0.name' = 'a'," +
			" 'format.fields.1.type' = 'STRING'," +
			" 'format.fields.1.name' = 'b'," +
			" 'format.fields.2.type' = 'INT'," +
			"'format.fields.2.name' = 'c'," +
			" 'format.fields.3.type' = 'STRING'," +
			" 'format.fields.3.name' = 'd'," +
			" 'format.fields.4.type' = 'STRING'," +
			" 'format.fields.4.name' = 'e'" +
			" " +
			" "+
			""+
			")";
		tableEnv.sqlUpdate(tableCsvSinkDDL);

		String sql = "INSERT INTO RubberOrders SELECT o.amount, lower(o.currency), r.amount,r.product, upper(r.product) \n" +
			"FROM Orders AS o   left join mysqlid FOR SYSTEM_TIME AS OF o.proctime as r\n" +
			"on  o.amount = 1 and o.currency = concat(r.product, 'r')" ;
		System.out.println(sql);

		tableEnv.sqlUpdate(sql);

		tableEnv.execute("s");

	}

}
