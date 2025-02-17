// Generated from com/example/query/parser/QueryLang.g4 by ANTLR 4.13.1
package com.example.query.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class QueryLangParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, SELECT=7, FROM=8, WHERE=9, 
		CONTAINS=10, NER=11, TEMPORAL=12, DEPENDENCY=13, ORDER=14, BY=15, ASC=16, 
		DESC=17, LIMIT=18, BEFORE=19, AFTER=20, BETWEEN=21, AND=22, STRING=23, 
		IDENTIFIER=24, NUMBER=25, WS=26, COMMENT=27;
	public static final int
		RULE_query = 0, RULE_identifier = 1, RULE_whereClause = 2, RULE_condition = 3, 
		RULE_containsExpression = 4, RULE_nerExpression = 5, RULE_temporalExpression = 6, 
		RULE_temporalSpec = 7, RULE_dependencyExpression = 8, RULE_variable = 9, 
		RULE_orderByClause = 10, RULE_orderSpec = 11, RULE_limitClause = 12;
	private static String[] makeRuleNames() {
		return new String[] {
			"query", "identifier", "whereClause", "condition", "containsExpression", 
			"nerExpression", "temporalExpression", "temporalSpec", "dependencyExpression", 
			"variable", "orderByClause", "orderSpec", "limitClause"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'documents'", "'('", "')'", "','", "'?'", "'*'", "'SELECT'", "'FROM'", 
			"'WHERE'", "'CONTAINS'", "'NER'", "'TEMPORAL'", "'DEPENDENCY'", "'ORDER'", 
			"'BY'", "'ASC'", "'DESC'", "'LIMIT'", "'BEFORE'", "'AFTER'", "'BETWEEN'", 
			"'AND'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, "SELECT", "FROM", "WHERE", 
			"CONTAINS", "NER", "TEMPORAL", "DEPENDENCY", "ORDER", "BY", "ASC", "DESC", 
			"LIMIT", "BEFORE", "AFTER", "BETWEEN", "AND", "STRING", "IDENTIFIER", 
			"NUMBER", "WS", "COMMENT"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "QueryLang.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public QueryLangParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryContext extends ParserRuleContext {
		public IdentifierContext source;
		public TerminalNode SELECT() { return getToken(QueryLangParser.SELECT, 0); }
		public TerminalNode FROM() { return getToken(QueryLangParser.FROM, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public OrderByClauseContext orderByClause() {
			return getRuleContext(OrderByClauseContext.class,0);
		}
		public LimitClauseContext limitClause() {
			return getRuleContext(LimitClauseContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(26);
			match(SELECT);
			setState(27);
			match(T__0);
			setState(28);
			match(FROM);
			setState(29);
			((QueryContext)_localctx).source = identifier();
			setState(31);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(30);
				whereClause();
				}
			}

			setState(34);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(33);
				orderByClause();
				}
			}

			setState(37);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(36);
				limitClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(QueryLangParser.IDENTIFIER, 0); }
		public TerminalNode STRING() { return getToken(QueryLangParser.STRING, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_identifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(39);
			_la = _input.LA(1);
			if ( !(_la==STRING || _la==IDENTIFIER) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WhereClauseContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(QueryLangParser.WHERE, 0); }
		public List<ConditionContext> condition() {
			return getRuleContexts(ConditionContext.class);
		}
		public ConditionContext condition(int i) {
			return getRuleContext(ConditionContext.class,i);
		}
		public WhereClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whereClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterWhereClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitWhereClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitWhereClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhereClauseContext whereClause() throws RecognitionException {
		WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_whereClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(41);
			match(WHERE);
			setState(43); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(42);
				condition();
				}
				}
				setState(45); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & 15364L) != 0) );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConditionContext extends ParserRuleContext {
		public ContainsExpressionContext containsExpression() {
			return getRuleContext(ContainsExpressionContext.class,0);
		}
		public NerExpressionContext nerExpression() {
			return getRuleContext(NerExpressionContext.class,0);
		}
		public TemporalExpressionContext temporalExpression() {
			return getRuleContext(TemporalExpressionContext.class,0);
		}
		public DependencyExpressionContext dependencyExpression() {
			return getRuleContext(DependencyExpressionContext.class,0);
		}
		public List<ConditionContext> condition() {
			return getRuleContexts(ConditionContext.class);
		}
		public ConditionContext condition(int i) {
			return getRuleContext(ConditionContext.class,i);
		}
		public ConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_condition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterCondition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitCondition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitCondition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConditionContext condition() throws RecognitionException {
		ConditionContext _localctx = new ConditionContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_condition);
		int _la;
		try {
			setState(59);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CONTAINS:
				enterOuterAlt(_localctx, 1);
				{
				setState(47);
				containsExpression();
				}
				break;
			case NER:
				enterOuterAlt(_localctx, 2);
				{
				setState(48);
				nerExpression();
				}
				break;
			case TEMPORAL:
				enterOuterAlt(_localctx, 3);
				{
				setState(49);
				temporalExpression();
				}
				break;
			case DEPENDENCY:
				enterOuterAlt(_localctx, 4);
				{
				setState(50);
				dependencyExpression();
				}
				break;
			case T__1:
				enterOuterAlt(_localctx, 5);
				{
				setState(51);
				match(T__1);
				setState(53); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(52);
					condition();
					}
					}
					setState(55); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & 15364L) != 0) );
				setState(57);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ContainsExpressionContext extends ParserRuleContext {
		public Token value;
		public TerminalNode CONTAINS() { return getToken(QueryLangParser.CONTAINS, 0); }
		public TerminalNode STRING() { return getToken(QueryLangParser.STRING, 0); }
		public ContainsExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_containsExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterContainsExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitContainsExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitContainsExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ContainsExpressionContext containsExpression() throws RecognitionException {
		ContainsExpressionContext _localctx = new ContainsExpressionContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_containsExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(61);
			match(CONTAINS);
			setState(62);
			match(T__1);
			setState(63);
			((ContainsExpressionContext)_localctx).value = match(STRING);
			setState(64);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NerExpressionContext extends ParserRuleContext {
		public IdentifierContext type;
		public IdentifierContext identTarget;
		public VariableContext varTarget;
		public TerminalNode NER() { return getToken(QueryLangParser.NER, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public NerExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nerExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterNerExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitNerExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitNerExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NerExpressionContext nerExpression() throws RecognitionException {
		NerExpressionContext _localctx = new NerExpressionContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_nerExpression);
		try {
			setState(80);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(66);
				match(NER);
				setState(67);
				match(T__1);
				setState(68);
				((NerExpressionContext)_localctx).type = identifier();
				setState(69);
				match(T__3);
				setState(70);
				((NerExpressionContext)_localctx).identTarget = identifier();
				setState(71);
				match(T__2);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(73);
				match(NER);
				setState(74);
				match(T__1);
				setState(75);
				((NerExpressionContext)_localctx).type = identifier();
				setState(76);
				match(T__3);
				setState(77);
				((NerExpressionContext)_localctx).varTarget = variable();
				setState(78);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TemporalExpressionContext extends ParserRuleContext {
		public TemporalSpecContext dateSpec;
		public TerminalNode TEMPORAL() { return getToken(QueryLangParser.TEMPORAL, 0); }
		public TemporalSpecContext temporalSpec() {
			return getRuleContext(TemporalSpecContext.class,0);
		}
		public TemporalExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_temporalExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterTemporalExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitTemporalExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitTemporalExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TemporalExpressionContext temporalExpression() throws RecognitionException {
		TemporalExpressionContext _localctx = new TemporalExpressionContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_temporalExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(82);
			match(TEMPORAL);
			setState(83);
			match(T__1);
			setState(84);
			((TemporalExpressionContext)_localctx).dateSpec = temporalSpec();
			setState(85);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TemporalSpecContext extends ParserRuleContext {
		public Token date;
		public Token start;
		public Token end;
		public TerminalNode BEFORE() { return getToken(QueryLangParser.BEFORE, 0); }
		public List<TerminalNode> STRING() { return getTokens(QueryLangParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(QueryLangParser.STRING, i);
		}
		public TerminalNode AFTER() { return getToken(QueryLangParser.AFTER, 0); }
		public TerminalNode BETWEEN() { return getToken(QueryLangParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(QueryLangParser.AND, 0); }
		public TemporalSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_temporalSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterTemporalSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitTemporalSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitTemporalSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TemporalSpecContext temporalSpec() throws RecognitionException {
		TemporalSpecContext _localctx = new TemporalSpecContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_temporalSpec);
		try {
			setState(95);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case BEFORE:
				enterOuterAlt(_localctx, 1);
				{
				setState(87);
				match(BEFORE);
				setState(88);
				((TemporalSpecContext)_localctx).date = match(STRING);
				}
				break;
			case AFTER:
				enterOuterAlt(_localctx, 2);
				{
				setState(89);
				match(AFTER);
				setState(90);
				((TemporalSpecContext)_localctx).date = match(STRING);
				}
				break;
			case BETWEEN:
				enterOuterAlt(_localctx, 3);
				{
				setState(91);
				match(BETWEEN);
				setState(92);
				((TemporalSpecContext)_localctx).start = match(STRING);
				setState(93);
				match(AND);
				setState(94);
				((TemporalSpecContext)_localctx).end = match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DependencyExpressionContext extends ParserRuleContext {
		public IdentifierContext governor;
		public IdentifierContext relation;
		public IdentifierContext dependent;
		public TerminalNode DEPENDENCY() { return getToken(QueryLangParser.DEPENDENCY, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public DependencyExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dependencyExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterDependencyExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitDependencyExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitDependencyExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DependencyExpressionContext dependencyExpression() throws RecognitionException {
		DependencyExpressionContext _localctx = new DependencyExpressionContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_dependencyExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(97);
			match(DEPENDENCY);
			setState(98);
			match(T__1);
			setState(99);
			((DependencyExpressionContext)_localctx).governor = identifier();
			setState(100);
			match(T__3);
			setState(101);
			((DependencyExpressionContext)_localctx).relation = identifier();
			setState(102);
			match(T__3);
			setState(103);
			((DependencyExpressionContext)_localctx).dependent = identifier();
			setState(104);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VariableContext extends ParserRuleContext {
		public Token name;
		public TerminalNode IDENTIFIER() { return getToken(QueryLangParser.IDENTIFIER, 0); }
		public VariableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_variable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterVariable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitVariable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitVariable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final VariableContext variable() throws RecognitionException {
		VariableContext _localctx = new VariableContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_variable);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(106);
			match(T__4);
			setState(107);
			((VariableContext)_localctx).name = match(IDENTIFIER);
			setState(109);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__4 || _la==T__5) {
				{
				setState(108);
				_la = _input.LA(1);
				if ( !(_la==T__4 || _la==T__5) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OrderByClauseContext extends ParserRuleContext {
		public TerminalNode ORDER() { return getToken(QueryLangParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(QueryLangParser.BY, 0); }
		public List<OrderSpecContext> orderSpec() {
			return getRuleContexts(OrderSpecContext.class);
		}
		public OrderSpecContext orderSpec(int i) {
			return getRuleContext(OrderSpecContext.class,i);
		}
		public OrderByClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderByClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterOrderByClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitOrderByClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitOrderByClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderByClauseContext orderByClause() throws RecognitionException {
		OrderByClauseContext _localctx = new OrderByClauseContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_orderByClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(111);
			match(ORDER);
			setState(112);
			match(BY);
			setState(113);
			orderSpec();
			setState(118);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(114);
				match(T__3);
				setState(115);
				orderSpec();
				}
				}
				setState(120);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OrderSpecContext extends ParserRuleContext {
		public IdentifierContext field;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ASC() { return getToken(QueryLangParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(QueryLangParser.DESC, 0); }
		public OrderSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterOrderSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitOrderSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitOrderSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderSpecContext orderSpec() throws RecognitionException {
		OrderSpecContext _localctx = new OrderSpecContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_orderSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(121);
			((OrderSpecContext)_localctx).field = identifier();
			setState(123);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(122);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LimitClauseContext extends ParserRuleContext {
		public Token count;
		public TerminalNode LIMIT() { return getToken(QueryLangParser.LIMIT, 0); }
		public TerminalNode NUMBER() { return getToken(QueryLangParser.NUMBER, 0); }
		public LimitClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limitClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterLimitClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitLimitClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitLimitClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LimitClauseContext limitClause() throws RecognitionException {
		LimitClauseContext _localctx = new LimitClauseContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_limitClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(125);
			match(LIMIT);
			setState(126);
			((LimitClauseContext)_localctx).count = match(NUMBER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\u0004\u0001\u001b\u0081\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
		"\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
		"\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
		"\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
		"\u0002\f\u0007\f\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0001"+
		"\u0000\u0003\u0000 \b\u0000\u0001\u0000\u0003\u0000#\b\u0000\u0001\u0000"+
		"\u0003\u0000&\b\u0000\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002"+
		"\u0004\u0002,\b\u0002\u000b\u0002\f\u0002-\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0004\u00036\b\u0003\u000b"+
		"\u0003\f\u00037\u0001\u0003\u0001\u0003\u0003\u0003<\b\u0003\u0001\u0004"+
		"\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005Q\b\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007`\b\u0007\u0001\b\u0001"+
		"\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\t\u0001"+
		"\t\u0001\t\u0003\tn\b\t\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0005"+
		"\nu\b\n\n\n\f\nx\t\n\u0001\u000b\u0001\u000b\u0003\u000b|\b\u000b\u0001"+
		"\f\u0001\f\u0001\f\u0001\f\u0000\u0000\r\u0000\u0002\u0004\u0006\b\n\f"+
		"\u000e\u0010\u0012\u0014\u0016\u0018\u0000\u0003\u0001\u0000\u0017\u0018"+
		"\u0001\u0000\u0005\u0006\u0001\u0000\u0010\u0011\u0082\u0000\u001a\u0001"+
		"\u0000\u0000\u0000\u0002\'\u0001\u0000\u0000\u0000\u0004)\u0001\u0000"+
		"\u0000\u0000\u0006;\u0001\u0000\u0000\u0000\b=\u0001\u0000\u0000\u0000"+
		"\nP\u0001\u0000\u0000\u0000\fR\u0001\u0000\u0000\u0000\u000e_\u0001\u0000"+
		"\u0000\u0000\u0010a\u0001\u0000\u0000\u0000\u0012j\u0001\u0000\u0000\u0000"+
		"\u0014o\u0001\u0000\u0000\u0000\u0016y\u0001\u0000\u0000\u0000\u0018}"+
		"\u0001\u0000\u0000\u0000\u001a\u001b\u0005\u0007\u0000\u0000\u001b\u001c"+
		"\u0005\u0001\u0000\u0000\u001c\u001d\u0005\b\u0000\u0000\u001d\u001f\u0003"+
		"\u0002\u0001\u0000\u001e \u0003\u0004\u0002\u0000\u001f\u001e\u0001\u0000"+
		"\u0000\u0000\u001f \u0001\u0000\u0000\u0000 \"\u0001\u0000\u0000\u0000"+
		"!#\u0003\u0014\n\u0000\"!\u0001\u0000\u0000\u0000\"#\u0001\u0000\u0000"+
		"\u0000#%\u0001\u0000\u0000\u0000$&\u0003\u0018\f\u0000%$\u0001\u0000\u0000"+
		"\u0000%&\u0001\u0000\u0000\u0000&\u0001\u0001\u0000\u0000\u0000\'(\u0007"+
		"\u0000\u0000\u0000(\u0003\u0001\u0000\u0000\u0000)+\u0005\t\u0000\u0000"+
		"*,\u0003\u0006\u0003\u0000+*\u0001\u0000\u0000\u0000,-\u0001\u0000\u0000"+
		"\u0000-+\u0001\u0000\u0000\u0000-.\u0001\u0000\u0000\u0000.\u0005\u0001"+
		"\u0000\u0000\u0000/<\u0003\b\u0004\u00000<\u0003\n\u0005\u00001<\u0003"+
		"\f\u0006\u00002<\u0003\u0010\b\u000035\u0005\u0002\u0000\u000046\u0003"+
		"\u0006\u0003\u000054\u0001\u0000\u0000\u000067\u0001\u0000\u0000\u0000"+
		"75\u0001\u0000\u0000\u000078\u0001\u0000\u0000\u000089\u0001\u0000\u0000"+
		"\u00009:\u0005\u0003\u0000\u0000:<\u0001\u0000\u0000\u0000;/\u0001\u0000"+
		"\u0000\u0000;0\u0001\u0000\u0000\u0000;1\u0001\u0000\u0000\u0000;2\u0001"+
		"\u0000\u0000\u0000;3\u0001\u0000\u0000\u0000<\u0007\u0001\u0000\u0000"+
		"\u0000=>\u0005\n\u0000\u0000>?\u0005\u0002\u0000\u0000?@\u0005\u0017\u0000"+
		"\u0000@A\u0005\u0003\u0000\u0000A\t\u0001\u0000\u0000\u0000BC\u0005\u000b"+
		"\u0000\u0000CD\u0005\u0002\u0000\u0000DE\u0003\u0002\u0001\u0000EF\u0005"+
		"\u0004\u0000\u0000FG\u0003\u0002\u0001\u0000GH\u0005\u0003\u0000\u0000"+
		"HQ\u0001\u0000\u0000\u0000IJ\u0005\u000b\u0000\u0000JK\u0005\u0002\u0000"+
		"\u0000KL\u0003\u0002\u0001\u0000LM\u0005\u0004\u0000\u0000MN\u0003\u0012"+
		"\t\u0000NO\u0005\u0003\u0000\u0000OQ\u0001\u0000\u0000\u0000PB\u0001\u0000"+
		"\u0000\u0000PI\u0001\u0000\u0000\u0000Q\u000b\u0001\u0000\u0000\u0000"+
		"RS\u0005\f\u0000\u0000ST\u0005\u0002\u0000\u0000TU\u0003\u000e\u0007\u0000"+
		"UV\u0005\u0003\u0000\u0000V\r\u0001\u0000\u0000\u0000WX\u0005\u0013\u0000"+
		"\u0000X`\u0005\u0017\u0000\u0000YZ\u0005\u0014\u0000\u0000Z`\u0005\u0017"+
		"\u0000\u0000[\\\u0005\u0015\u0000\u0000\\]\u0005\u0017\u0000\u0000]^\u0005"+
		"\u0016\u0000\u0000^`\u0005\u0017\u0000\u0000_W\u0001\u0000\u0000\u0000"+
		"_Y\u0001\u0000\u0000\u0000_[\u0001\u0000\u0000\u0000`\u000f\u0001\u0000"+
		"\u0000\u0000ab\u0005\r\u0000\u0000bc\u0005\u0002\u0000\u0000cd\u0003\u0002"+
		"\u0001\u0000de\u0005\u0004\u0000\u0000ef\u0003\u0002\u0001\u0000fg\u0005"+
		"\u0004\u0000\u0000gh\u0003\u0002\u0001\u0000hi\u0005\u0003\u0000\u0000"+
		"i\u0011\u0001\u0000\u0000\u0000jk\u0005\u0005\u0000\u0000km\u0005\u0018"+
		"\u0000\u0000ln\u0007\u0001\u0000\u0000ml\u0001\u0000\u0000\u0000mn\u0001"+
		"\u0000\u0000\u0000n\u0013\u0001\u0000\u0000\u0000op\u0005\u000e\u0000"+
		"\u0000pq\u0005\u000f\u0000\u0000qv\u0003\u0016\u000b\u0000rs\u0005\u0004"+
		"\u0000\u0000su\u0003\u0016\u000b\u0000tr\u0001\u0000\u0000\u0000ux\u0001"+
		"\u0000\u0000\u0000vt\u0001\u0000\u0000\u0000vw\u0001\u0000\u0000\u0000"+
		"w\u0015\u0001\u0000\u0000\u0000xv\u0001\u0000\u0000\u0000y{\u0003\u0002"+
		"\u0001\u0000z|\u0007\u0002\u0000\u0000{z\u0001\u0000\u0000\u0000{|\u0001"+
		"\u0000\u0000\u0000|\u0017\u0001\u0000\u0000\u0000}~\u0005\u0012\u0000"+
		"\u0000~\u007f\u0005\u0019\u0000\u0000\u007f\u0019\u0001\u0000\u0000\u0000"+
		"\u000b\u001f\"%-7;P_mv{";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}