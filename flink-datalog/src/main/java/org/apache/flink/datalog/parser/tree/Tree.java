package org.apache.flink.datalog.parser.tree;

public interface Tree {
	<T> T accept(TreeVisitor<? extends T> visitor);
}
