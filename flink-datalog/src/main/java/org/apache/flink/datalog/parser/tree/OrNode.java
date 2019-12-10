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
public class OrNode extends Node {
	private PredicateData predicateData;
	private List<AndNode> children = new ArrayList<>();

	public OrNode(PredicateData predicateData) {
		this.predicateData = predicateData;
	}

	@Override
	public List<AndNode> getChildren() {
		return children;
	}

	public void setChildren(List<AndNode> children) {
		this.children = children;
	}

	@Override
	public int getChildCount() {
		return this.children.size();
	}

	@Override
	public Node getChild(int i) {
		return this.children.get(i);
	}

	@Override
	public PredicateData getPredicateData() {
		return predicateData;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		OrNode orNode = (OrNode) o;
		return predicateData.equals(orNode.predicateData);
	}

	@Override
	public String toString() {
		return "OrNode{" +
			"predicateData=" + predicateData +
			", children=" + children +
			'}';
	}

	@Override
	public <T> void accept(TreeVisitor<? extends T> visitor) {
		if (visitor instanceof AndOrTreeVisitor) {
			((AndOrTreeVisitor) visitor).visitOrNode(this);
		} else {
			visitor.visitChildren(this);
		}
	}
}
