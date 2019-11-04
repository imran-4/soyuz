package org.apache.flink.datalog.plan.logical;

import org.apache.flink.datalog.parser.tree.AndNode;
import org.apache.flink.datalog.parser.tree.OrNode;

public class AndOrTreeBaseVisitor<T> extends AbstractTreeVisitor<T> implements AndOrTreeVisitor<T> {
	@Override
	public T visitOrNode(OrNode orNode) {
		return null;
	}

	@Override
	public T visitAndNode(AndNode andNode) {
		return null;
	}
}
