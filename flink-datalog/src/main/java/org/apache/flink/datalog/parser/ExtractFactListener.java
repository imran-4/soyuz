package org.apache.flink.datalog.parser;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.flink.datalog.DatalogBaseListener;
import org.apache.flink.datalog.DatalogParser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ExtractFactListener extends DatalogBaseListener {
	private Map<String, List<String>> facts = new LinkedHashMap<>();
	@Override
	public void enterFact(DatalogParser.FactContext ctx) {
		String factName = ctx.factName().getText();
		List<String> constants = new ArrayList<>();
		for (TerminalNode constant : ctx.constantList().CONSTANT()) {
			constants.add(constant.getText());
		}
		this.facts.put(factName, constants);
	}

	public Map<String, List<String>> getFacts() {
		return this.facts;
	}
}
