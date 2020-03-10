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
public class PrimitivePredicateData extends PredicateData {
	private TermData leftTerm;
	private String operator;
	private TermData rightTerm;

	public PrimitivePredicateData(TermData leftTerm, String operator, TermData rightTerm) {
		this.leftTerm = leftTerm;
		this.operator = operator;
		this.rightTerm = rightTerm;
	}

	public TermData getLeftTerm() {
		return leftTerm;
	}

	public String getOperator() {
		return operator;
	}

	public TermData getRightTerm() {
		return rightTerm;
	}

	@Override
	public String getPredicateName() {
		return null;
	}

	@Override
	public List<TermData> getPredicateParameters() {
		return null;
	}

	@Override
	public String toString() {
		return "PrimitivePredicateData{" +
			"leftTerm=" + leftTerm +
			", operator='" + operator + '\'' +
			", rightTerm=" + rightTerm +
			'}';
	}

	public enum Operators {
		EQUALS,
		NOT_EQUALS,
		LESS_THAN,
		GREATER_THAN,
		//...
	}
}
