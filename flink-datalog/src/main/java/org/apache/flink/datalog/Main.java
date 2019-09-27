package org.apache.flink.datalog;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.flink.datalog.parser.AstBuilder;
import org.apache.flink.datalog.parser.ExtractDatabaseSchemaListener;
import org.apache.flink.datalog.parser.ExtractQueryListener;

public class Main {
	//for testing only
	public static void main(String[] args) {

		String schema = "database({abc_Table(x: Integer, y:Integer)}).";
		String inputProgram =
			"abc(X,Y) :- abcTable(X, Y).\n" +
			"abc(X,Y) :- abc(A,X),xyz(Y,B)."
			;

		CharStream input = CharStreams.fromString(schema);
		DatalogLexer lexer = new DatalogLexer(input);
		TokenStream tokens = new CommonTokenStream(lexer);
		DatalogParser parser = new DatalogParser(tokens);

		ParseTree tree = parser.compileUnit(); // parse the content and get the tree

		ParseTreeWalker walker = new ParseTreeWalker();
		ExtractDatabaseSchemaListener listener = new ExtractDatabaseSchemaListener();
		walker.walk(listener, tree);

		String query = "abc(X,Y)?";

		CharStream inputQuery = CharStreams.fromString(query);
		DatalogLexer queryLexer = new DatalogLexer(inputQuery);
		TokenStream queryTokens = new CommonTokenStream(queryLexer);
		DatalogParser queryParser = new DatalogParser(queryTokens);

		ParseTree queryTree = queryParser.query(); // parse the content and get the tree

		ParseTreeWalker queryWalker = new ParseTreeWalker();

		ExtractQueryListener queryListener = new ExtractQueryListener();
		queryWalker.walk(queryListener, queryTree);

//		AstBuilder builder = new AstBuilder();
//		builder.visit(tree);

	}
}
