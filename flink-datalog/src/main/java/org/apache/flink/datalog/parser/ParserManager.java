package org.apache.flink.datalog.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.flink.datalog.DatalogLexer;
import org.apache.flink.datalog.DatalogParser;

import java.util.List;

public class ParserManager {

	public void parseCompileUnit(String program) {
		parse(program, ParsableTypes.COMPILEUNIT);
	}

	public void parseFact(String fact) {
		parse(fact, ParsableTypes.FACT);
	}

	public void parseRuleClause(String rule) {
		parse(rule, ParsableTypes.RULE);
	}

	public void parsePredicate(String predicate) {
		parse(predicate, ParsableTypes.PREDICATE);
	}

	public void parseQuery(String query) {
		parse(query, ParsableTypes.QUERY);
	}

	public void parseDatabase(String databaseInfo) {
		parse(databaseInfo, ParsableTypes.DATABASE);
	}

	private ParseTree parse(String program, ParsableTypes type) {
		CharStream input = CharStreams.fromString(program);
		DatalogLexer lexer = new DatalogLexer(input);
		TokenStream tokens = new CommonTokenStream(lexer);
		DatalogParser parser = new DatalogParser(tokens);

		parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION); //make the parser report all ambiguities

		parser.removeErrorListeners(); //remove ConsoleErrorListener
		parser.addErrorListener(new DatalogErrorListener());
		parser.setErrorHandler(new DefaultErrorStrategy());


		ParseTree tree = null;
		switch (type) {
			case COMPILEUNIT:
				tree = parser.compileUnit();
				break;
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
				System.out.println("Unrecognized parser rule."); // will take care of it later..
				break;
		}
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
		} else {
			//create AST here may be. or return the parse tree to the caller and the caller will decide what to do with it.


			return tree;

		}
	}
}
