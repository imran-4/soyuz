package org.apache.flink.datalog.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RuleSet;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.plan.Optimizer;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.rules.FlinkRuleSets;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;
import scala.Function0;

public class DatalogBatchOptimizer extends Optimizer {
	PlanningConfigurationBuilder planningConfigurationBuilder;
	Function0<CalciteConfig> calciteConfig;

	public DatalogBatchOptimizer(Function0<CalciteConfig> calciteConfig, PlanningConfigurationBuilder planningConfigurationBuilder) {
		super(calciteConfig, planningConfigurationBuilder);
		this.calciteConfig = calciteConfig;
		this.planningConfigurationBuilder = planningConfigurationBuilder;

	}

	public RelNode optimize(RelNode relNode) {
		var convSubQueryPlan = optimizeConvertSubQueries(relNode);
		var expandedPlan = optimizeExpandPlan(convSubQueryPlan);
		var decorPlan = RelDecorrelator.decorrelateQuery(expandedPlan);
		var normalizedPlan = optimizeNormalizeLogicalPlan(decorPlan);
		var logicalPlan = optimizeLogicalPlan(normalizedPlan);
		return optimizePhysicalPlan(logicalPlan, FlinkConventions.DATASET());
	}

	@Override
	public RuleSet getBuiltInNormRuleSet() {
		return FlinkRuleSets.DATASET_NORM_RULES();
	}

	@Override
	public RuleSet getBuiltInPhysicalOptRuleSet() {
		return FlinkRuleSets.DATASET_OPT_RULES();
	}
}
