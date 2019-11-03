package org.apache.flink.datalog.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.datalog.DatalogLexer;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.datalog.parser.graph.Edge;
import org.apache.flink.datalog.parser.graph.PrecedenceGraphBuilder;
import org.apache.flink.datalog.parser.graph.Vertex;
import org.apache.flink.datalog.parser.tree.AndOrTree;
import org.apache.flink.datalog.parser.tree.Node;
import org.apache.flink.datalog.parser.tree.RelTreeBuilder;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.jgrapht.Graph;

import java.util.List;

public class ParserManager {
	private RelTreeBuilder builder;

	public ParserManager(FlinkRelBuilder flinkRelBuilder) {
		this.builder = new RelTreeBuilder(flinkRelBuilder);
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

	public RelNode parse(String inputProgram, String query) {
		String program = inputProgram +
			System.getProperty("line.separator") +
			query;
		ParseTree programTree = this.parse(program);

//		AndOrTree graphBuilder = new AndOrTree();
//		assert programTree != null;
//		Node rootNode = graphBuilder.visit(programTree);

		PrecedenceGraphBuilder graphBuilder = new PrecedenceGraphBuilder();
		assert programTree != null;
		Graph<Vertex, Edge> graph = graphBuilder.visit(programTree);



		return null;
	}
}
