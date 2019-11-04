package org.apache.flink.datalog.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.flink.datalog.DatalogLexer;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.datalog.parser.tree.AndOrTree;
import org.apache.flink.datalog.parser.tree.Node;

import java.util.List;

public class ParserManager {
	public ParserManager() {

	}

	private ParseTree parse(String text) {
		CharStream input = CharStreams.fromString(text);
		DatalogLexer lexer = new DatalogLexer(input);
		TokenStream tokens = new CommonTokenStream(lexer);
		DatalogParser parser = new DatalogParser(tokens);

		parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION); //make the parser report all ambiguities

		parser.removeErrorListeners(); //remove ConsoleErrorListener
		parser.addErrorListener(new DatalogErrorListener());
		parser.setErrorHandler(new DefaultErrorStrategy());

		ParseTree tree = parser.compileUnit();
		if (tree == null) return null;
		int numberOfErrors = parser.getNumberOfSyntaxErrors();
		if (numberOfErrors > 0) {
			List<? extends ANTLRErrorListener> errorListeners = parser.getErrorListeners();
			for (ANTLRErrorListener errorListener : errorListeners) {
				if (errorListener instanceof DatalogErrorListener) {
					List<String> syntaxErrors = ((DatalogErrorListener) errorListener).getSyntaxErrors();
					for (String error : syntaxErrors)
						System.out.println(error);
				}
			}
			return null; //will take care of it later
		}
		return tree;

	}

	//	public Graph<Vertex, Edge> parse(String inputProgram, String query) {
	public Node parse(String inputProgram, String query) {
		String program = inputProgram +
			System.getProperty("line.separator") +
			query;
		ParseTree programTree = this.parse(program);

		AndOrTree treeBuilder = new AndOrTree();
		assert programTree != null;
		return treeBuilder.visit(programTree);

//		PrecedenceGraphBuilder graphBuilder = new PrecedenceGraphBuilder();
//		assert programTree != null;
//		Graph<Vertex, Edge> graph = graphBuilder.visit(programTree);
	}
}
