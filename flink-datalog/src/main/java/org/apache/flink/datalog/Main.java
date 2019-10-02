package org.apache.flink.datalog;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.flink.datalog.tree.AST;
import org.apache.flink.datalog.tree.AstBuilder;

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


		String inputProgram =
				"abc(X,Y) :- abcTable(X, Y).\n" +
				"abc(X,Y) :- abc(A,X),xyz(Y,B)."
			;

		CharStream input = CharStreams.fromString(inputProgram);
		DatalogLexer lexer = new DatalogLexer(input);
		TokenStream tokens = new CommonTokenStream(lexer);
		DatalogParser parser = new DatalogParser(tokens);

		ParseTree tree = parser.compileUnit(); // parse the content and get the tree

		AstBuilder builder = new AstBuilder();
		AST ast = builder.visit(tree);
		System.out.println(ast.toString());

	}
}
