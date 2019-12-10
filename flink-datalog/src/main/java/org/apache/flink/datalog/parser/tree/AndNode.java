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

package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.parser.tree.predicate.PredicateData;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class AndNode extends Node {
	private PredicateData predicateData;
	private List<OrNode> children = new ArrayList<>();
	private boolean isRecursive = false;

	public AndNode(PredicateData predicateData) {
		this.predicateData = predicateData;
	}

	public AndNode(PredicateData predicateData, boolean isRecursive) {
		this.predicateData = predicateData;
		this.isRecursive = isRecursive;
	}

	public boolean isRecursive() {
		return this.isRecursive;
	}

	public void setRecursive(boolean isRecursive) {
		this.isRecursive = isRecursive;
	}

	@Override
	public List<OrNode> getChildren() {
		return children;
	}

	void setChildren(List<OrNode> children) {
		this.children = children;
	}

	@Override
	public PredicateData getPredicateData() {
		return predicateData;
	}

	@Override
	public int getChildCount() {
		return this.children.size();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		AndNode andNode = (AndNode) o;
		return predicateData.equals(andNode.predicateData);
	}

	@Override
	public Node getChild(int i) {
		return this.children.get(i);
	}

	@Override
	public String toString() {
		return "AndNode{" +
			"predicateData=" + predicateData +
			", children=" + children +
			", isRecursive=" + isRecursive +
			'}';
	}

	@Override
	public <T> void accept(TreeVisitor<? extends T> visitor) {
		if (visitor instanceof AndOrTreeVisitor) {
			((AndOrTreeVisitor) visitor).visitAndNode(this);
		} else {
			visitor.visitChildren(this);
		}
	}
}
