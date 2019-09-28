package org.apache.flink.datalog.parser;

import org.apache.flink.datalog.DatalogBaseListener;
import org.apache.flink.datalog.DatalogParser;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;

/*
* This class is used to extract the provided schema.
*
* */

public class ExtractDatabaseSchemaListener extends DatalogBaseListener {
	private Map<String, Map<String, String>> schema = new HashMap<>();

	public ExtractDatabaseSchemaListener() {
	}

	@Override
	public void enterDatabase(DatalogParser.DatabaseContext ctx) {
		enterSchema(ctx.schema());
	}

	@Override
	public void enterSchema(DatalogParser.SchemaContext ctx) {
		int numberOfTables = ctx.tableName().size();
		for (int i = 0; i < numberOfTables; i++) {
			String tableName = ctx.tableName(i).getText();
			DatalogParser.ColumnsListContext columnListCtx = ctx.columnsList(i);
			int numberOfColumnsInCurrentTable = columnListCtx.columnName().size();
			Map<String, String> columnsDataTypeMap = new LinkedHashMap<>();
			for (int j = 0; j < numberOfColumnsInCurrentTable; j++) {
				columnsDataTypeMap.put(columnListCtx.columnName(j).getText(), columnListCtx.columnDataType(j).getText());
			}
			schema.put(tableName, columnsDataTypeMap);
		}
		System.out.println(schema.toString());
	}

	public Map<String, Map<String, String>> getDatabaseSchema() {
		return this.schema;
	}
}
