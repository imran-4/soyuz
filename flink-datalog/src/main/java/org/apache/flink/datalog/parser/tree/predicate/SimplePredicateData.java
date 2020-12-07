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

package org.apache.flink.datalog.parser.tree.predicate;

import java.util.List;

/**
 *
 */
public class SimplePredicateData extends PredicateData {
	private final String predicateName;
	private final List<TermData<?>> predicateParameters;
	private boolean isIdb = false;

	public SimplePredicateData(String predicateName, List<TermData<?>> predicateParameters) {
		this.predicateName = predicateName;
		this.predicateParameters = predicateParameters;
		this.isIdb = false;
	}

	public SimplePredicateData(
		String predicateName,
		List<TermData<?>> predicateParameters,
		boolean isIdb) {
		this(predicateName, predicateParameters);
		this.isIdb = isIdb;
	}

	public boolean isIdb() {
		return isIdb;
	}

	public void setIdb(boolean idb) {
		isIdb = idb;
	}

	@Override
	public String getPredicateName() {
		return predicateName;
	}

	@Override
	public List<TermData<?>> getPredicateParameters() {
		return predicateParameters;
	}

	@Override
	public String toString() {
		return "SimplePredicateData{" +
			"predicateName='" + predicateName + '\'' +
			", predicateParameters=" + predicateParameters +
			", isIdb=" + isIdb +
			'}';
	}
}
