package org.apache.flink.datalog.tree;

import java.util.ArrayList;
import java.util.List;

public class VariableNode extends AbstractTreeNode {

	private String variableName;

	public String getVariableName() {
		return variableName;
	}

	public void setVariableName(String variableName) {
		this.variableName = variableName;
	}

	@Override
	public List<TreeNode> getChildren() {
		return null;
	}

	@Override
	public List<TreeNode> getSiblings() {
		return null;
	}
}
