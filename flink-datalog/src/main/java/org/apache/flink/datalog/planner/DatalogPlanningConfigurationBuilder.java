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

package org.apache.flink.datalog.planner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.datalog.DatalogEnvironment;
import org.apache.flink.datalog.planner.calcite.FlinkDatalogPlannerImpl;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkRelBuilderFactory;
import org.apache.flink.table.calcite.FlinkRelOptClusterFactory;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.catalog.BasicOperatorTable;
import org.apache.flink.table.catalog.CatalogReader;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.FunctionCatalogOperatorTable;
import org.apache.flink.table.codegen.ExpressionReducer;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.plan.cost.DataSetCostFactory;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;
import org.apache.flink.table.util.JavaScalaConversionUtil;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Utility class to create {@link org.apache.calcite.tools.RelBuilder} or {@link FrameworkConfig} used to create
 * a {@link org.apache.calcite.tools.Planner} implementation for Datalog.
 */
@Internal
public class DatalogPlanningConfigurationBuilder extends PlanningConfigurationBuilder {
    private final RelOptCostFactory costFactory = new DataSetCostFactory();
    private final RelDataTypeSystem typeSystem = new FlinkTypeSystem();
    private final FlinkTypeFactory typeFactory = new FlinkTypeFactory(typeSystem);
    private final RelOptPlanner planner;
    private final ExpressionBridge<PlannerExpression> expressionBridge;
    private final Context context;
    private final TableConfig tableConfig;
    private final FunctionCatalog functionCatalog;
    private CalciteSchema rootSchema;
    private DatalogEnvironment datalogEnvironment;

    public DatalogPlanningConfigurationBuilder(
            TableConfig tableConfig,
            FunctionCatalog functionCatalog,
            CalciteSchema rootSchema,
            ExpressionBridge<PlannerExpression> expressionBridge, DatalogEnvironment datalogEnvironment) {
        super(tableConfig, functionCatalog, rootSchema, expressionBridge);
        this.tableConfig = tableConfig;
        this.functionCatalog = functionCatalog;
        this.context = super.getContext();
        this.planner = super.getPlanner();
        this.expressionBridge = expressionBridge;
        this.rootSchema = rootSchema;
        this.datalogEnvironment = datalogEnvironment;
    }

    /**
     * Creates a configured {@link FlinkRelBuilder} for a planning session.
     *
     * @param currentCatalog  the current default catalog to look for first during planning.
     * @param currentDatabase the current default database to look for first during planning.
     * @return configured rel builder
     */
    @Override
    public FlinkRelBuilder createRelBuilder(String currentCatalog, String currentDatabase) {
//        RelOptCluster cluster = FlinkRelOptClusterFactory.create(
//                this.planner,
//                new RexBuilder(typeFactory));
//
//        RelOptSchema relOptSchema = createCatalogReader(false, currentCatalog, currentDatabase);
//
//        return new FlinkRelBuilder(context, cluster, relOptSchema, expressionBridge);
		RelOptCluster cluster = FlinkRelOptClusterFactory.create(
				planner,
				new RexBuilder(typeFactory));
		RelOptSchema relOptSchema = createCatalogReader(false, currentCatalog, currentDatabase);

		return new FlinkRelBuilder(context, cluster, relOptSchema, expressionBridge);
    }

    @Override
    public FlinkDatalogPlannerImpl createFlinkPlanner(String currentCatalog, String currentDatabase) {
        return new FlinkDatalogPlannerImpl(
                createFrameworkConfig(),
                isLenient -> createCatalogReader(isLenient, currentCatalog, currentDatabase),
                this.planner,
                typeFactory);
    }

    /**
     * Returns the {@link FlinkTypeFactory} that will be used.
     */
    public FlinkTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public Context getContext() {
        return context;
    }

    /**
     * Returns the SQL parser config for this environment including a custom Calcite configuration.
     */
    public SqlParser.Config getSqlParserConfig() {
        return JavaScalaConversionUtil.toJava(calciteConfig(tableConfig).sqlParserConfig()).orElseGet(() ->
                // we use Java lex because back ticks are easier than double quotes in programming
                // and cases are preserved
                SqlParser
                        .configBuilder()
                        .setParserFactory(FlinkSqlParserImpl.FACTORY)
                        .setConformance(getSqlConformance())
                        .setLex(Lex.JAVA)
                        .build());
    }

    private FlinkSqlConformance getSqlConformance() {
        SqlDialect sqlDialect = tableConfig.getSqlDialect();
        switch (sqlDialect) {
            case HIVE:
                return FlinkSqlConformance.HIVE;
            case DEFAULT:
                return FlinkSqlConformance.DEFAULT;
            default:
                throw new TableException("Unsupported SQL dialect: " + sqlDialect);
        }
    }

    private CatalogReader createCatalogReader(
            boolean lenientCaseSensitivity,
            String currentCatalog,
            String currentDatabase) {
        SqlParser.Config sqlParserConfig = getSqlParserConfig();
        final boolean caseSensitive;
        if (lenientCaseSensitivity) {
            caseSensitive = false;
        } else {
            caseSensitive = sqlParserConfig.caseSensitive();
        }

        SqlParser.Config parserConfig = SqlParser.configBuilder(sqlParserConfig)
                .setCaseSensitive(caseSensitive)
                .build();

        return new CatalogReader(
                rootSchema,
                asList(
                        asList(currentCatalog, currentDatabase),
                        singletonList(currentCatalog)
                ),
                typeFactory,
                CalciteConfig.connectionConfig(parserConfig));
    }

//	private FrameworkConfig createFrameworkConfig() {
//		return Frameworks
//			.newConfigBuilder()
//			.costFactory(costFactory)
//			.typeSystem(typeSystem)
//			.defaultSchema(this.rootSchema.plus())
//			.operatorTable(getSqlOperatorTable(calciteConfig(tableConfig), functionCatalog))
//			.sqlToRelConverterConfig(
//				getSqlToRelConverterConfig(
//					calciteConfig(tableConfig),
//					expressionBridge))
//			.executor(new ExpressionReducer(tableConfig))
//			.build();
//	}

    private FrameworkConfig createFrameworkConfig() {
        return Frameworks
                .newConfigBuilder()
                .parserConfig(getSqlParserConfig())
                .costFactory(costFactory)
                .typeSystem(typeSystem)
                .operatorTable(getSqlOperatorTable(calciteConfig(tableConfig), functionCatalog))
                .sqlToRelConverterConfig(
                        getSqlToRelConverterConfig(
                                calciteConfig(tableConfig),
                                expressionBridge))
                // set the executor to evaluate constant expressions
                .executor(new ExpressionReducer(tableConfig))
                .build();
    }

    private CalciteConfig calciteConfig(TableConfig tableConfig) {
        return tableConfig.getPlannerConfig()
                .unwrap(CalciteConfig.class)
                .orElseGet(CalciteConfig::DEFAULT);
    }

    /**
     * Returns the {@link SqlToRelConverter} config.
     */
    private SqlToRelConverter.Config getSqlToRelConverterConfig(
            CalciteConfig calciteConfig,
            ExpressionBridge<PlannerExpression> expressionBridge) {
        return JavaScalaConversionUtil.toJava(calciteConfig.sqlToRelConverterConfig()).orElseGet(
                () -> SqlToRelConverter.configBuilder()
                        .withTrimUnusedFields(false)
                        .withConvertTableAccess(true)
                        .withInSubQueryThreshold(Integer.MAX_VALUE)
                        .withRelBuilderFactory(new FlinkRelBuilderFactory(expressionBridge))
                        .build()
        );
    }

    /**
     * Returns the operator table for this environment including a custom Calcite configuration.
     */
    private SqlOperatorTable getSqlOperatorTable(CalciteConfig calciteConfig, FunctionCatalog functionCatalog) {
        SqlOperatorTable baseOperatorTable = ChainedSqlOperatorTable.of(
                new BasicOperatorTable(),
                new FunctionCatalogOperatorTable(functionCatalog, typeFactory)
        );

        return JavaScalaConversionUtil.toJava(calciteConfig.sqlOperatorTable()).map(operatorTable -> {
                    if (calciteConfig.replacesSqlOperatorTable()) {
                        return operatorTable;
                    } else {
                        return ChainedSqlOperatorTable.of(baseOperatorTable, operatorTable);
                    }
                }
        ).orElse(baseOperatorTable);
    }
}
