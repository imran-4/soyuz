package org.apache.flink.datalog.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

class DatalogErrorListener implements ANTLRErrorListener {
	private List<String> syntaxErrors = new ArrayList<>();


	@Override
	public void syntaxError(Recognizer<?, ?> recognizer, Object o, int line, int position, String message, RecognitionException recognitionException) {
		List<String> stack = ((Parser)recognizer).getRuleInvocationStack();
		String errorMessage = "Error occured on line number " + line + " at position " + position + ". The message is: " + message + ".";
		syntaxErrors.add(errorMessage);
	}

	@Override
	public void reportAmbiguity(Parser parser, DFA dfa, int i, int i1, boolean b, BitSet bitSet, ATNConfigSet atnConfigSet) {

	}

	@Override
	public void reportAttemptingFullContext(Parser parser, DFA dfa, int i, int i1, BitSet bitSet, ATNConfigSet atnConfigSet) {

	}

	@Override
	public void reportContextSensitivity(Parser parser, DFA dfa, int i, int i1, int i2, ATNConfigSet atnConfigSet) {

	}

	public List<String> getSyntaxErrors() {
		return this.syntaxErrors;
	}
}
