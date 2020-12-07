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

package org.apache.flink.datalog.executor;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.delegation.BatchExecutor;

import java.util.List;

/**
 *
 */
public class DatalogBatchExecutor extends BatchExecutor {
	public DatalogBatchExecutor(StreamExecutionEnvironment executionEnvironment) {
		super(executionEnvironment);
	}

	//    @Override
//    public void apply(List<Transformation<?>> transformations) {
//        this.transformations.addAll(transformations);
//
//    }


	/**
	 * Translates the given transformations to a Pipeline.
	 *
	 * @param transformations list of transformations
	 * @param tableConfig
	 * @param jobName what should be the name of the job
	 *
	 * @return The pipeline representing the transformations.
	 */
	@Override
	public Pipeline createPipeline(
		List<Transformation<?>> transformations,
		TableConfig tableConfig,
		String jobName) {
		return null;
	}

//	/**
//	 * Executes the given pipeline.
//	 *
//	 * @param pipeline the pipeline to execute
//	 * @return The result of the job execution, containing elapsed time and accumulators.
//	 * @throws Exception which occurs during job execution.
//	 */
//	@Override
//	public JobExecutionResult execute(Pipeline pipeline) throws Exception {
//		return super.getExecutionEnvironment().execute(pipeline);
//	}

	/**
	 * Executes the given pipeline asynchronously.
	 *
	 * @param pipeline the pipeline to execute
	 *
	 * @return A {@link JobClient} that can be used to communicate with the submitted job,
	 * 	completed on submission succeeded.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	@Override
	public JobClient executeAsync(Pipeline pipeline) throws Exception {
		return null;
	}
}
