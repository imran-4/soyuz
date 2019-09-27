package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datalog.BatchDatalogEnvironment;
import org.apache.flink.datalog.DatalogEnvironment;

public class TransitiveClosureTest {
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env);

		DataSet<String> ds = env.readTextFile("");    ///or alternatively define another data source to read data from. or use Table API

		DataSet<Tuple2<Integer, Integer>> edges = ds.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> map(String value) throws Exception {
				String[] elements = value.split(",");
				return new Tuple2<Integer, Integer>(Integer.parseInt(elements[0]), Integer.parseInt(elements[1]));
			}
		});

		String datalogProgram = "database({graph(x: Integer, y:Integer)}).\n" +
			"abc(X,Y) :- graph(X, Y).\n" +
			"abc(X,Y) :- abc(X,Y), graph(Y,Z).\n";

		datalogEnv.compile(datalogProgram); //this should only convert program to flink operator(s), so that they can be executed lazily upon calling of an sink operator

		DataSet<Tuple2<Long, Long>> tc = (DataSet<Tuple2<Long, Long>>) datalogEnv.query("abc(X,Y)?");
		try {
			tc.collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
