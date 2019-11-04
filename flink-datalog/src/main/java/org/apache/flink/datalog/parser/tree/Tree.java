package org.apache.flink.datalog.parser.tree;

import org.apache.flink.datalog.plan.logical.TreeVisitor;

public interface Tree {
	<T> T accept(TreeVisitor<? extends T> visitor);
}
