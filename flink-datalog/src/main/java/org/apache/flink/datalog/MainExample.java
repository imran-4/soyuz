package org.apache.flink.datalog;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.types.IntValue;

import static org.apache.flink.table.api.Expressions.$;

public class MainExample {
	public static void main(String[] args) throws Exception {
		String testFilePath = null;

		if (args.length > 0) {
			testFilePath = args[0].trim();
		} else
			throw new Exception("Please provide input dataset. ");
		String inputProgram =
			"tc(X,Y) :- arc(X,Y).\n"
				+ "tc(X,Y) :- tc(X,Z),arc(Z,Y).\n";
		String query = "tc(X,Y)?";


//		String inputProgram = "triangles(X, Y, Z) :- arc(X, Y), X < Y, arc(Y, Z), Y < Z, arc(Z, X)."
//			+ "counttriangles(count<~>):-triangles(X, Y, Z).";
//		String query = "counttriangles(X)?";
//
//		String inputProgram = "sg(X,Y):-arc(P,X),arc(P,Y),X!=Y.\n"
//			+ "sg(X,Y):-arc(A,X),sg(A,B),arc(B,Y).\n";
//
//		String query = "sg(X,Y)?";

//		String inputProgram = "sssp2(Y, mmin<D>) :- Y=1, D=0.\n"
//			+ "sssp2(Y, mmin<D>) :- sssp2(X, D1), arc(X, Y, D2), D = D1 + D2.\n"
//			+ "sssp(X, min<D>) :- sssp2(X, D).";
//
//		String query = "sssp(X, Y)?";

//		String inputProgram = "cc2(X,mmin<X>):-arc(X,Y).\n"
//			+ "cc2(Y,mmin<Z>):-cc2(X,Z),arc(X,Y).\n"
//			+ "cc(X,min<Y>):-cc2(X,Y).\n";
//
//		String query = "cc(X,Y)?";

//		String inputProgram = "outdegree(X,count<Y>) :- edge(X,Y).\n"
//			+ "pageRank(X,0,1).\n"
//			+ "pageRank(X,I+ 1,sum<P/D>) :- edge(Y,X),pageRank(Y,I,P),outdegree(Y,D),I<20.\n";
//
//		String query = "pageRank(X,Y,Z)?";

//		String inputProgram = "uarc(X, Y) :- arc(X, Y).\n"
//			+ "uarc(Y, X) :- arc(X, Y).\n"
//			+ "cnt(Y, Z, count<X>) :- uarc(X, Y), uarc(X, Z), Y!= Z, ~uarc(Y, Z).\n"
//			+ "pymk(X, W9, topk<10, Z>) :- cnt(X, 15, Z), pages(X, W2,W3, W9).";
//
//		String query = "pymk(X,Y,Z)?";

//		String inputProgram = "cntComing(Y, mcount<X>) :- attend(X), friend(Y, X).\n"
//			+ "attend(X) :- organizer(X).\n"
//			+ "attend(X) :- cntComing(X, N), N >= 3.\n";
//		String query = "attend(X)?";

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.useDatalogPlanner()
			.inBatchMode()
			.build();
		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
		DataSet<Tuple2<IntValue, IntValue>> dataSet = env
			.readCsvFile(testFilePath)
			.fieldDelimiter(",")
			.types(IntValue.class, IntValue.class);

		datalogEnv.createTemporaryView("arc", dataSet, $("v1"),$("v2"));

		Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
		DataSet<Tuple2<IntValue, IntValue>> resultDS = datalogEnv.toDataSet(
			queryResult,
			dataSet.getType());
		resultDS.writeAsCsv(testFilePath + "_output");
		System.out.println(resultDS.count());
	}
}
