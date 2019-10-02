package org.apache.flink.datalog.parser;

import org.apache.flink.datalog.DatalogBaseListener;
import org.apache.flink.datalog.DatalogParser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

//this is not important for now...will see it later...
public class ExtractRetractionListener extends DatalogBaseListener {
	private Map<String, List<String>> predicates = new LinkedHashMap<>();

	@Override
	public void enterRetraction(DatalogParser.RetractionContext ctx) {
		String predicateName = ctx.predicate().predicateName().getText();
		List<String> terms = new ArrayList<>();

		for (DatalogParser.TermContext t : ctx.predicate().termList().term()) {
			terms.add(t.getText());
		}
		this.predicates.put(predicateName, terms);
	}

	public Map<String, List<String>> getPredicates() {
		return this.predicates;
	}
}
