package org.apache.flink.datalog.parser.graph;

public class TermData {
	private String termName;
	private Adornment adornment;

	public TermData(String termName, Adornment adornment) {
		this.termName = termName;
		this.adornment = adornment;
	}

	public String getTermName() {
		return termName;
	}

	public Adornment getAdornment() {
		return adornment;
	}

	void setAdornment(Adornment adornment) {
		this.adornment = adornment;
	}

	@Override
	public String toString() {
		return "TermData{" +
			"termName='" + termName + '\'' +
			", adornment=" + adornment +
			'}';
	}

	public enum Adornment {
		BOUND,
		FREE
	}
}
