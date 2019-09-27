package org.apache.flink.datalog.parser;

import org.apache.flink.datalog.DatalogBaseListener;
import org.apache.flink.datalog.DatalogParser;

import java.util.*;

public class ExtractQueryListener extends DatalogBaseListener {
	private String tabĺeName = null;
	private Map<String, String> columnNameTypeMapping= new LinkedHashMap<>();

	@Override
	public void enterQuery(DatalogParser.QueryContext ctx) {
		ExtractPredicateListener predicateListener =  new ExtractPredicateListener();
		predicateListener.enterPredicate(ctx.predicate());

		this.tabĺeName = predicateListener.getTabĺeName() ;
		this.columnNameTypeMapping = predicateListener.getColumnNameTypeMapping();
	}

	public String getTabĺeName() {
		return tabĺeName;
	}

	public Map<String, String> getColumns() {
		return columnNameTypeMapping;
	}
}
