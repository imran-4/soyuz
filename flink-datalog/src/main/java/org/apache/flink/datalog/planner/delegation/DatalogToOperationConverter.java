package org.apache.flink.datalog.planner.delegation;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.operations.PlannerQueryOperation;


public class DatalogToOperationConverter {
	private FlinkPlannerImpl flinkPlanner;

	private DatalogToOperationConverter(FlinkPlannerImpl flinkPlanner) {
		this.flinkPlanner = flinkPlanner;
	}

	public static Operation convert(FlinkPlannerImpl flinkPlanner, RelRoot relRoot) {
		// validate the query
//		final RelNode validated = flinkPlanner.validate(sqlNode);
		DatalogToOperationConverter converter = new DatalogToOperationConverter(flinkPlanner);
//		if (validated instanceof SqlCreateTable) {
//			return converter.convertCreateTable((SqlCreateTable) validated);
//		} if (validated instanceof SqlDropTable) {
//			return converter.convertDropTable((SqlDropTable) validated);
//		} else if (validated instanceof RichSqlInsert) {
//			return converter.convertSqlInsert((RichSqlInsert) validated);
//		} else if (validated.getKind().belongsTo(SqlKind.QUERY)) {
//			return converter.convertSqlQuery(validated);
//		} else {
//			throw new TableException("Unsupported node type "
//				+ validated.getClass().getSimpleName());
//		}

		return  null;
	}

	private Operation convertSqlQuery(RelRoot relRoot) {
		return toQueryOperation(flinkPlanner, relRoot);
	}

	private Operation toQueryOperation(FlinkPlannerImpl flinkPlanner, RelRoot relRoot) {
//		RelRoot relational = flinkPlanner.rel(validated);
		return new PlannerQueryOperation(relRoot.rel);

	}


}
