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

/**
 *
 */
public class TermData<T> { //todo: add type information if needed...
	private T term;
	private Adornment adornment;

	public TermData(T term, Adornment adornment) {
		this.term = term;
		this.adornment = adornment;
	}

	@Override
	public String toString() {
		return "TermData{" +
			"termName='" + term + '\'' +
			", adornment=" + adornment +
			'}';
	}

	public T getTerm() {
		return term;
	}

	public Adornment getAdornment() {
		return adornment;
	}

	public void setAdornment(Adornment adornment) {
		this.adornment = adornment;
	}

	public enum Adornment {
		BOUND,
		FREE
	}
}
