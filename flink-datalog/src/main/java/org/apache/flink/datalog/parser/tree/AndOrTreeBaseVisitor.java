package org.apache.flink.datalog.parser.tree;

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
