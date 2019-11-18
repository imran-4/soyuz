package org.apache.flink.datalog;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;

public class Main {
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useDatalogPlanner().inBatchMode().withBuiltInCatalogName("my_catalog").withBuiltInDatabaseName("my_database").build();
		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
		DataSource<Tuple2<String, String>> dataSet = env.fromElements(
			new Tuple2<>("a", "b"),
			new Tuple2<>("b", "c"),
			new Tuple2<>("c", "c"),
			new Tuple2<>("c", "d")); //will also support loading data using facts. e.g., fact(a,b). fact(b,c).
		datalogEnv.registerDataSet("graph", dataSet, "v1,v2"); //register EDB. EDBs are registered explicitly.
		DataSource<Tuple2<String, String>> dataSet1 = env.fromElements(
			new Tuple2<>("b", "1"),
			new Tuple2<>("c", "1"),
			new Tuple2<>("c", "1"),
			new Tuple2<>("d", "1")); //just to test the query logical plan...will remove it later
		datalogEnv.registerDataSet("tc", dataSet1, "v2,v3");

		String inputProgram =
			"tc(X,Y) :- graph(X,Y).\n" +
				"tc(X,Y) :- tc(X,Z),graph(Z,Y).";
		String query = "tc(X,Y)?";
		String inputProgram1 =
			"msic(SI,SZ) :- su(S1,s3), su(S2, S3), mt(S3).\n" +
				"msic(Sl,S2) :- au(SI,P), au(SU2,P).\n" +
				"msic(SI,Sf) :- au(S1, PI), au(S2, PZ), ci(PI, SZ), ci(P2, SI).\n" +
				"msic(SI,St) :- au(Sl, PI), au(S2, PZ), ci(PI,P2), ci(P2, Pl).\n" +
				"know(S2, R, T) :- orig(S1, R, T), msic(S1, S2).\n" +
				"sif(Sl,SZ, Xi, T) :- at(S1, M, T), at(S2, M, T), cnf(M, X, T).\n" +
				"knowl(L, R, T) :- cra(S, L, T), know(S, R, T).\n" +
				"know(S2, R, T) :- know(S1, R, T), abt(R, X), sif(S1, S2, R, T)."; // only for testing the graph
		String query1 = "know(X,Y,Z)?";

		String inputProgram2 = "sg(X,Y):-arc(P,X),arc(P,Y),X!=Y.\n" +
			"arc1(X,Y):- arc(Z,Y).\n" +
			"arc1(X,Y):- arc1(X,Z), arc(Z,Y).\n" +
			"sg(X,Y):-arc(A,X),sg(A,B),arc1(B,Y).\n";
		String query2 = "sg(X,Y)?";

		datalogEnv.registerDataSet("arc", dataSet1, "v1,v2");

		String inputProgram3 = "sg(X,Y):-arc(P,X),arc(P,Y),X!=Y.\n" +
			"sg(X,Y):-arc(A,X),sg(A,B),arc(X,Y).\n";
		String query3 = "sg(X,Y)?";


		String inputProgram4 = "";
		String query4 = "arc(X,Y)?"; // simple "select v1,v2 from graph   " query (no recursion involved).

		Table queryResult = datalogEnv.datalogQuery(inputProgram4, query4);
		try {
			DataSet<Tuple2<String, String>> dataSet2 = datalogEnv.toDataSet(queryResult, dataSet1.getType());
			dataSet2.print();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
