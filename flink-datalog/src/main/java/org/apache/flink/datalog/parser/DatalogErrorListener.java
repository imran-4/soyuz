/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.datalog.parser;

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 *
 */
class DatalogErrorListener implements ANTLRErrorListener {
	private final List<String> syntaxErrors = new ArrayList<>();


	@Override
	public void syntaxError(
		Recognizer<?, ?> recognizer,
		Object o,
		int line,
		int position,
		String message,
		RecognitionException recognitionException) {
		List<String> stack = ((Parser) recognizer).getRuleInvocationStack();
		String errorMessage = "Error occured on line number " + line + " at position " + position
			+ ". The message is: " + message + ".";
		syntaxErrors.add(errorMessage);
	}

	@Override
	public void reportAmbiguity(
		Parser parser,
		DFA dfa,
		int i,
		int i1,
		boolean b,
		BitSet bitSet,
		ATNConfigSet atnConfigSet) {

	}

	@Override
	public void reportAttemptingFullContext(
		Parser parser,
		DFA dfa,
		int i,
		int i1,
		BitSet bitSet,
		ATNConfigSet atnConfigSet) {

	}

	@Override
	public void reportContextSensitivity(
		Parser parser,
		DFA dfa,
		int i,
		int i1,
		int i2,
		ATNConfigSet atnConfigSet) {

	}

	public List<String> getSyntaxErrors() {
		return this.syntaxErrors;
	}
}
