package org.apache.flink.datalog.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.datalog.DatalogLexer;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.datalog.parser.tree.RelTreeBuilder;

import java.util.List;

public class ParserManager {

	public RelNode parseCompileUnit(String program) {
		return parse(program, ParsableTypes.COMPILEUNIT);
	}

	public RelNode parseFact(String fact) {
		return parse(fact, ParsableTypes.FACT);
	}

	public RelNode parseRuleClause(String rule) {
		return parse(rule, ParsableTypes.RULE);
	}

	public RelNode parsePredicate(String predicate) {
		return parse(predicate, ParsableTypes.PREDICATE);
	}

	public RelNode parseQuery(String query) {
		return parse(query, ParsableTypes.QUERY);
	}

	private RelNode parse(String program, ParsableTypes type) {
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
			case RULE:
				tree = parser.ruleClause();
				break;
			case PREDICATE:
				tree = parser.predicate();
				break;
			default:
				System.out.println("Unrecognized parser rule."); // will take care of it later..

				break;
		}
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
		} else {
			//create AST or RelNode (of Calcite) here...
			RelTreeBuilder builder = new RelTreeBuilder();
			RelNode ast = builder.visit(tree);
			System.out.println(ast.toString());
			return ast;
		}
	}
}
