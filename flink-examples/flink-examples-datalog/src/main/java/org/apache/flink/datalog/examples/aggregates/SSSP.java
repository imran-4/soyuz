package org.apache.flink.datalog.examples.aggregates;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datalog.BatchDatalogEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.IntValue;

public class SSSP {
	public static void main(String[] args) throws Exception {
		String testFilePath = null;

		if (args.length > 0) {
			testFilePath = args[0].trim();
		} else
			throw new Exception("Please provide input dataset. ");
		String inputProgram = "sssp2(Y, mmin<D>) :- Y=1, D=0.\n"
			+ "sssp2(Y, mmin<D>) :- sssp2(X, D1), arc(X, Y, D2), D = D1 + D2.\n"
			+ "sssp(X, min<D>) :- sssp2(X, D).";
		if (args.length == 3) {
			inputProgram =
				"sssp2(Y, mmin<D>) :- Y=" + new IntValue(Integer.parseInt(args[1])) + ", D=" + new IntValue(Integer.parseInt(args[2])) + ".\n"
					+ "sssp2(Y, mmin<D>) :- sssp2(X, D1), arc(X, Y, D2), D = D1 + D2.\n"
					+ "sssp(X, min<D>) :- sssp2(X, D).";
		}
		String query = "sssp(X,Y)?";
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.useDatalogPlanner()
			.inBatchMode()
			.build();
		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
		DataSet<Tuple2<Integer, Integer>> dataSet = env.readCsvFile(testFilePath).fieldDelimiter(",").types(Integer.class, Integer.class);

		datalogEnv.registerDataSet("arc", dataSet, "v1,v2");
		Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
		DataSet<Tuple2<Integer, Integer>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());

//        resultDS.writeAsCsv(testFilePath+"_output");
		System.out.println(resultDS.count());
	}
}
