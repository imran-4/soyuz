package org.apache.flink.datalog.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.datalog.DatalogLexer;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.datalog.parser.tree.RelTreeBuilder;
import org.apache.flink.datalog.parser.tree.graph.GraphBuilder;
import org.apache.flink.table.calcite.FlinkRelBuilder;

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
			for (int i = 0; i < errorListeners.size(); i++) {
				if (errorListeners.get(i) instanceof DatalogErrorListener) {
					List<String> syntaxErrors = ((DatalogErrorListener) errorListeners.get(i)).getSyntaxErrors();
					for (String error : syntaxErrors)
						System.out.println(error);
				}
			}
			return null; //will take care of it later
		}
		return tree;

	}

	public RelNode parse(String inputProgram, String query) {
		ParseTree inputProgramTree = this.parse(inputProgram);
		ParseTree queryTree = this.parse(query);

		//create AST or RelNode (of Calcite) here.....
//		RelNode programRelNode = builder.visit(inputProgramTree);
//		RelNode queryRelNode = builder.visit(inputProgramTree);
//		return queryRelNode;

		GraphBuilder graphBuilder = new GraphBuilder();
		graphBuilder.visit(inputProgramTree);
		return null;
	}
}
