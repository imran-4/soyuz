package org.apache.flink.datalog.planner;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.datalog.parser.ParsableTypes;
import org.apache.flink.datalog.parser.ParserManager;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.util.ArrayList;
import java.util.List;

public class FlinkDatalogPlanner implements Planner {
	private ParsableTypes programType = null;


	public FlinkDatalogPlanner() {
		//change it later
//		super(null,null,null,null,false);
	}

	public FlinkDatalogPlanner(Executor executor, TableConfig config, FunctionCatalog functionCatalog, CatalogManager catalogManager, boolean isStreamingMode) {

	}

	public void setParsableType(ParsableTypes parsableType) {
		programType = parsableType;
	}

	@Override
	public List<Operation> parse(String text) {
		// need a mechanism in this method to distinguish between query and rule
		ParserManager parserManager = new ParserManager();
		parserManager.parse(text, this.programType);



//		SqlNode parsed = planner.parse(rules);
		//		parsed match {
//			case insert: RichSqlInsert =>
//				List(SqlToOperationConverter.convert(planner, insert))
//			case query if query.getKind.belongsTo(SqlKind.QUERY) =>
//				List(SqlToOperationConverter.convert(planner, query))
//			case ddl if ddl.getKind.belongsTo(SqlKind.DDL) =>
//				List(SqlToOperationConverter.convert(planner, ddl))
//			case _ =>
//				throw new TableException(s"Unsupported query: $stmt")
//		}
		return null;
	}

	@Override
	public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
		if (modifyOperations.isEmpty()) {
			new ArrayList<Transformation<?>>();
		}
//		mergeParameters();
//		var relNodes = modifyOperations.map(translateToRel);
//		var optimizedRelNodes = optimize(relNodes);
//		var execNodes = translateToExecNodePlan(optimizedRelNodes);
//		translateToPlan(execNodes);
		return null;
	}

	@Override
	public String explain(List<Operation> operations, boolean extended) {
		return null;
	}

	@Override
	public String[] getCompletionHints(String statement, int position) {
		return new String[0];
	}
}
