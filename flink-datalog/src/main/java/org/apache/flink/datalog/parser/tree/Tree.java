package org.apache.flink.datalog.parser.tree;

public interface Tree {
	<T> void accept(TreeVisitor<? extends T> visitor);
}
