package org.apache.flink.datalog.parser.tree;

public class PredicateAST extends AbstractAST {
	private String text;
	private NodeType typ;

	@Override
	public void initialize(NodeType t, String txt) {
		setText(txt);
		setType(t);
	}

	@Override
	public String getText() {
		return this.text;
	}

	@Override
	public void setText(String text) {
		this.text = text;
	}

	@Override
	public NodeType getType() {
		return this.typ;
	}

	@Override
	public void setType(NodeType typ) {
		this.typ = typ;
	}
}
