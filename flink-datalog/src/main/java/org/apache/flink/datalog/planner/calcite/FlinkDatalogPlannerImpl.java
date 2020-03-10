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

package org.apache.flink.datalog.planner.calcite;

import org.apache.flink.datalog.parser.ParserManager;
import org.apache.flink.datalog.parser.tree.Node;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.catalog.CatalogReader;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.tools.FrameworkConfig;

import java.util.function.Function;

/**
 *
 */
public class FlinkDatalogPlannerImpl extends FlinkPlannerImpl {

	/**
	 *
	 * @param frameworkConfig
	 * @param catalogReaderSupplier
	 * @param planner
	 * @param typeFactory
	 */
    public FlinkDatalogPlannerImpl(
            FrameworkConfig frameworkConfig,
            Function<Boolean, CatalogReader> catalogReaderSupplier,
            RelOptPlanner planner,
            FlinkTypeFactory typeFactory) {
        super(frameworkConfig, catalogReaderSupplier, planner, typeFactory);

    }

	/**
	 *
	 * @param inputProgram
	 * @param query
	 * @return
	 */
    public Node parse(String inputProgram, String query) {
        try {
            ParserManager parserManager = new ParserManager();
            return parserManager.parse(inputProgram, query);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
