/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.datalog.plan;

import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.plan.Optimizer;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.rules.FlinkRuleSets;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RuleSet;

import scala.Function0;

/**
 *
 */
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
