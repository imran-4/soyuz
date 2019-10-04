package org.apache.flink.datalog;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class Main {
	//for testing only
	public static void main(String[] args) {
//
//		String database = "database({" +
//			"employee(EmployeeId:integer, DepartmentId:integer, FirstName:string, LastName:string)," +
//			"department(DepartmentId:integer, DepartmentName:string)" +
//			"}).";
//
//
//
//		CharStream input = CharStreams.fromString(database);
//		DatalogLexer lexer = new DatalogLexer(input);
//		TokenStream tokens = new CommonTokenStream(lexer);
//		DatalogParser parser = new DatalogParser(tokens);
//
//		ParseTree tree = parser.database(); // parse the content and get the tree
//
//		ParseTreeWalker walker = new ParseTreeWalker();
//		ExtractDatabaseSchemaListener listener = new ExtractDatabaseSchemaListener();
//		walker.walk(listener, tree);
//
//		String query = "abc(X,Y)?";
//
//		CharStream inputQuery = CharStreams.fromString(query);
//		DatalogLexer queryLexer = new DatalogLexer(inputQuery);
//		TokenStream queryTokens = new CommonTokenStream(queryLexer);
//		DatalogParser queryParser = new DatalogParser(queryTokens);
//
//		ParseTree queryTree = queryParser.query(); // parse the content and get the tree
//
//		ParseTreeWalker queryWalker = new ParseTreeWalker();
//
//		ExtractQueryListener queryListener = new ExtractQueryListener();
//		queryWalker.walk(queryListener, queryTree);

//
//		String inputProgram =
//				"abc(X,Y) :- abcTable(X, Y).\n" +
//				"abc(X,Y) :- abc(A,X),xyz(Y,B)."
//			;
//
//		CharStream input = CharStreams.fromString(inputProgram);
//		DatalogLexer lexer = new DatalogLexer(input);
//		TokenStream tokens = new CommonTokenStream(lexer);
//		DatalogParser parser = new DatalogParser(tokens);
//
//		ParseTree tree = parser.compileUnit(); // parse the content and get the tree
//
//		RelTreeBuilder builder = new RelTreeBuilder();
//		RelNode ast = builder.visit(tree);
//		System.out.println(ast.toString());

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env);
		DataSource<Tuple2<String, String>>  dataSet = env.fromElements( new Tuple2<String, String>("a", "b"),new Tuple2<String, String>("b", "c"),new Tuple2<String, String>("c", "c"),new Tuple2<String, String>("c", "d"));
		datalogEnv.registerDataSet("graph", dataSet, "v1, v2");
		String inputProgram =
			"abc(X,Y) :- graph(X, Y).\n" +
				"abc(X,Y) :- abc(X,Z),graph(Z,Y).";

		datalogEnv.datalogRules(inputProgram);
		DataSet<Tuple2<String,String>> queryResult = datalogEnv.datalogQuery("abc(X,Y)?");
		try {
			queryResult.collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
