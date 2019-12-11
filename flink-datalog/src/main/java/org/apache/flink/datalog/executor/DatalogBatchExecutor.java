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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import java.util.List;

/**
 *
 */
public class DatalogBatchExecutor extends ExecutorBase {
    public DatalogBatchExecutor(StreamExecutionEnvironment executionEnvironment) {
        super(executionEnvironment);
    }

    @Override
    public void apply(List<Transformation<?>> transformations) {
        this.transformations.addAll(transformations);

    }

    @Override
    public JobExecutionResult execute(String jobName) throws Exception {
        StreamExecutionEnvironment execEnv = getExecutionEnvironment();
        StreamGraph streamGraph = generateStreamGraph(jobName);
        return execEnv.execute(streamGraph);
    }

    private void setBatchProperties(StreamExecutionEnvironment execEnv) {
        ExecutionConfig executionConfig = execEnv.getConfig();
        executionConfig.enableObjectReuse();
        executionConfig.setLatencyTrackingInterval(-1);
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        execEnv.setBufferTimeout(-1);
        if (isShuffleModeAllBatch()) {
            executionConfig.setDefaultInputDependencyConstraint(InputDependencyConstraint.ALL);
        }
    }

    @Override
    public StreamGraph generateStreamGraph(List<Transformation<?>> transformations, String jobName) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private boolean isShuffleModeAllBatch() {
        String value = tableConfig.getConfiguration().getString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE);
        if (value.equalsIgnoreCase(ShuffleMode.BATCH.toString())) {
            return true;
        } else if (!value.equalsIgnoreCase(ShuffleMode.PIPELINED.toString())) {
            throw new IllegalArgumentException(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE.key() +
                    " can only be set to " + ShuffleMode.BATCH.toString() + " or " + ShuffleMode.PIPELINED.toString());
        }
        return false;
    }
}
