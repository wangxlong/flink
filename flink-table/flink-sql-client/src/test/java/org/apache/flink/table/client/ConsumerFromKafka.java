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


public class ConsumerFromKafka {

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


		String tableSourceConnectot = "create table kafkasource(\n" +
			"                       a DOUBLE,\n" +
			"                        b varchar,\n" +
			"                        c int,\n" +
			"                        d varchar,\n" +
			"                        e int\n" +
			"                      ) with (\n" +
			"                        'connector.type' = 'kafka',       \n" +
			"\n" +
			"  'connector.version' = '0.10'," +
			"\n" +
			"  'connector.topic' = 'topic_name',\n" +
			"\n" +
			"  'update-mode' = 'append',         \n" +
			"   'connector.property-version' = '1', \n" +
			"\n" +
			"  'connector.properties.0.key' = 'zookeeper.connect', \n" +
			"  'connector.properties.0.value' = 'localhost:2181',\n" +
			"  'connector.properties.1.key' = 'bootstrap.servers',\n" +
			"  'connector.properties.1.value' = 'localhost:9092',\n" +
			"  'connector.properties.2.key' = 'group.id',\n" +
			"  'connector.properties.2.value' = 'testGroup',\n" +
			"  'connector.startup-mode' = 'earliest-offset'   " +
			"\n" +
			"\n" +
			")";



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
			" 'format.fields.4.name' = 'e'," +
			"  'update-mode' = 'append' " +
			" "+
			""+
			"                      )";



		String tableMysqlSink = "create table fiveFields(\n" +
			"                       a DOUBLE,\n" +
			"                        b varchar,\n" +
			"                        c int,\n" +
			"                        d varchar,\n" +
			"                        e int\n" +
			"                      ) with (\n" +
			"                        'connector.type' = 'jdbc',\n" +
			" 'connector.url' = 'jdbc:mysql://localhost:3306/whl?serverTimezone=GMT'," +
			" 'connector.table' = 'fiveFields'," +
			" 'connector.username' = 'root'," +
			" 'connector.driver' = 'com.mysql.jdbc.Driver'," +
			" 'connector.write.flush.max-rows' = '2'," +
			" 'connector.password' = 'wanglin000'" +
			")";


		String[] fieldNames = {"a", "b", "c", "d", "e"};
		TypeInformation[] fieldTypes = {Types.DOUBLE, Types.STRING, Types.INT, Types.STRING, Types.LONG};
		tableEnv.registerTableSink("aaaa", new MyRetractSink(fieldNames, fieldTypes));

		tableEnv.sqlUpdate(tableSourceConnectot);

		tableEnv.sqlUpdate(tableMysqlSink);

//			String sql = "INSERT INTO aaaa SELECT * FROM Orders AS o left join Rate AS r on r.currency = o.currency and o.amount = 2 ";
//		tableEnv.sqlUpdate(sql);

		String sql = "INSERT INTO fiveFields SELECT user, currency, amount, currency, amount FROM kafkasource ";
		tableEnv.sqlUpdate(sql);

		bsEnv.execute("m");
	}

}
