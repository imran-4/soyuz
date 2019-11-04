package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.parser.tree.predicate.PredicateData;

import java.util.List;

public abstract class Node implements Tree {

	public abstract List<? extends Node> getChildren();

	public abstract PredicateData getPredicateData();

	public abstract int getChildrenCount();

}
