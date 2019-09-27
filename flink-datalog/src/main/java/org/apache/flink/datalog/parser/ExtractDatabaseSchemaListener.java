package org.apache.flink.datalog.parser;

import org.apache.flink.datalog.DatalogBaseListener;
import org.apache.flink.datalog.DatalogParser;

import java.util.HashMap;
import java.util.Map;

/*
* This class is used to extract the provided schema.
*
* */

public class ExtractDatabaseSchemaListener extends DatalogBaseListener {
	private String tableName = null;
	private Map<String, String> schema = new HashMap<>();

	public ExtractDatabaseSchemaListener() {
	}

	@Override
	public void enterSchema(DatalogParser.SchemaContext ctx) {
		this.tableName = ctx.tableName().CONSTANT().getText();
		int variableListSize = ctx.columnsList().columnName().size();
		for (int i = 0; i < variableListSize; i++) {
			this.schema.put(ctx.columnsList().columnName(i).getText(), ctx.columnsList().columnDataType(0).getText());
		}
	}

	public String getTableName() {
		return this.tableName;
	}

	public Map<String, String> getDatabaseSchema() {
		return this.schema;
	}
}
