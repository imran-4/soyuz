package org.apache.flink.datalog.parser;

import org.apache.flink.datalog.DatalogBaseListener;
import org.apache.flink.datalog.DatalogParser;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ExtractPredicateListener extends DatalogBaseListener {
	private String tabĺeName = null;
	private Map<String, String> columnNameTypeMapping= new LinkedHashMap<>();
	@Override
	public void enterPredicate(DatalogParser.PredicateContext ctx) {
		this.tabĺeName = ctx.predicateName().getText() ;

		List<DatalogParser.TermContext> terms = ctx.termList().term();
		for (DatalogParser.TermContext term : terms) {
			columnNameTypeMapping.put(term.VARIABLE().getText(), "VARIABLE"); //do this for other types as well
		}
	}

	String getTabĺeName() {
		return tabĺeName;
	}

	Map<String, String> getColumnNameTypeMapping() {
		return columnNameTypeMapping;
	}
}
