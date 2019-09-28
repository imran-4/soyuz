package org.apache.flink.datalog.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.flink.datalog.DatalogLexer;
import org.apache.flink.datalog.DatalogParser;

import java.util.List;
public class ParserManager {

	private void parseProgram(String program, ParsableTypes type) {
		CharStream input = CharStreams.fromString(program);
		DatalogLexer lexer = new DatalogLexer(input);
		TokenStream tokens = new CommonTokenStream(lexer);
		DatalogParser parser = new DatalogParser(tokens);

		if (parser.getErrorListeners().size() == 0) {
			parser.addErrorListener(new DatalogErrorListener());
			parser.setErrorHandler(new DefaultErrorStrategy());
		}

		ParseTree tree = null;
		switch (type) {
			case DATABASE:
				tree = parser.database();
				break;
			case RULE:
				tree = parser.ruleClause();
				break;
			case FACT:
				tree = parser.fact();
				break;
			case QUERY:
				tree = parser.query();
				break;
			case PREDICATE:
				tree = parser.predicate();
				break;
			default:
				return;
		}

		int numberOfErrors = parser.getNumberOfSyntaxErrors();
		if (numberOfErrors > 0) {
			List<? extends ANTLRErrorListener > errorListeners = parser.getErrorListeners();
			for (int i = 0; i < errorListeners.size(); i++) {
				if (errorListeners.get(i) instanceof DatalogErrorListener) {
					List <String> syntaxErrors = ((DatalogErrorListener) errorListeners.get(i)).getSyntaxErrors();
					for (String error: syntaxErrors)
						System.out.println(error);
				}
			}
		} else {
			//create AST here may be. or return the parse tree to the caller and the caller will decide what to do with it.
		}
	}
}
