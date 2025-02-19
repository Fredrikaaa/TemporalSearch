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
		CONTAINS=10, NER=11, TEMPORAL=12, DATE=13, DEPENDENCY=14, ORDER=15, BY=16, 
		ASC=17, DESC=18, LIMIT=19, BEFORE=20, AFTER=21, BETWEEN=22, NEAR=23, AND=24, 
		VARIABLE=25, WILDCARD=26, OPTIONAL=27, STRING=28, IDENTIFIER=29, NUMBER=30, 
		WS=31, COMMENT=32;
	public static final int
		RULE_query = 0, RULE_identifier = 1, RULE_whereClause = 2, RULE_condition = 3, 
		RULE_containsExpression = 4, RULE_nerExpression = 5, RULE_nerType = 6, 
		RULE_nerTarget = 7, RULE_temporalExpression = 8, RULE_temporalSpec = 9, 
		RULE_temporalValue = 10, RULE_temporalOperator = 11, RULE_dateValue = 12, 
		RULE_dependencyExpression = 13, RULE_depComponent = 14, RULE_variable = 15, 
		RULE_variableModifier = 16, RULE_subQuery = 17, RULE_orderByClause = 18, 
		RULE_orderSpec = 19, RULE_limitClause = 20;
	private static String[] makeRuleNames() {
		return new String[] {
			"query", "identifier", "whereClause", "condition", "containsExpression", 
			"nerExpression", "nerType", "nerTarget", "temporalExpression", "temporalSpec", 
			"temporalValue", "temporalOperator", "dateValue", "dependencyExpression", 
			"depComponent", "variable", "variableModifier", "subQuery", "orderByClause", 
			"orderSpec", "limitClause"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'documents'", "'('", "')'", "','", "'{'", "'}'", "'SELECT'", "'FROM'", 
			"'WHERE'", "'CONTAINS'", "'NER'", "'TEMPORAL'", "'DATE'", "'DEPENDENCY'", 
			"'ORDER'", "'BY'", "'ASC'", "'DESC'", "'LIMIT'", "'BEFORE'", "'AFTER'", 
			"'BETWEEN'", "'NEAR'", "'AND'", null, "'*'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, "SELECT", "FROM", "WHERE", 
			"CONTAINS", "NER", "TEMPORAL", "DATE", "DEPENDENCY", "ORDER", "BY", "ASC", 
			"DESC", "LIMIT", "BEFORE", "AFTER", "BETWEEN", "NEAR", "AND", "VARIABLE", 
			"WILDCARD", "OPTIONAL", "STRING", "IDENTIFIER", "NUMBER", "WS", "COMMENT"
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
			setState(42);
			match(SELECT);
			setState(43);
			match(T__0);
			setState(44);
			match(FROM);
			setState(45);
			((QueryContext)_localctx).source = identifier();
			setState(47);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(46);
				whereClause();
				}
			}

			setState(50);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(49);
				orderByClause();
				}
			}

			setState(53);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(52);
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
			setState(55);
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
			setState(57);
			match(WHERE);
			setState(59); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(58);
				condition();
				}
				}
				setState(61); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & 31780L) != 0) );
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
		public SubQueryContext subQuery() {
			return getRuleContext(SubQueryContext.class,0);
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
			setState(76);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CONTAINS:
				enterOuterAlt(_localctx, 1);
				{
				setState(63);
				containsExpression();
				}
				break;
			case NER:
				enterOuterAlt(_localctx, 2);
				{
				setState(64);
				nerExpression();
				}
				break;
			case TEMPORAL:
			case DATE:
				enterOuterAlt(_localctx, 3);
				{
				setState(65);
				temporalExpression();
				}
				break;
			case DEPENDENCY:
				enterOuterAlt(_localctx, 4);
				{
				setState(66);
				dependencyExpression();
				}
				break;
			case T__4:
				enterOuterAlt(_localctx, 5);
				{
				setState(67);
				subQuery();
				}
				break;
			case T__1:
				enterOuterAlt(_localctx, 6);
				{
				setState(68);
				match(T__1);
				setState(70); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(69);
					condition();
					}
					}
					setState(72); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & 31780L) != 0) );
				setState(74);
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
			setState(78);
			match(CONTAINS);
			setState(79);
			match(T__1);
			setState(80);
			((ContainsExpressionContext)_localctx).value = match(STRING);
			setState(81);
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
		public NerTypeContext type;
		public NerTargetContext target;
		public TerminalNode NER() { return getToken(QueryLangParser.NER, 0); }
		public NerTypeContext nerType() {
			return getRuleContext(NerTypeContext.class,0);
		}
		public NerTargetContext nerTarget() {
			return getRuleContext(NerTargetContext.class,0);
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
			enterOuterAlt(_localctx, 1);
			{
			setState(83);
			match(NER);
			setState(84);
			match(T__1);
			setState(85);
			((NerExpressionContext)_localctx).type = nerType();
			setState(86);
			match(T__3);
			setState(87);
			((NerExpressionContext)_localctx).target = nerTarget();
			setState(88);
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
	public static class NerTypeContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode WILDCARD() { return getToken(QueryLangParser.WILDCARD, 0); }
		public NerTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nerType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterNerType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitNerType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitNerType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NerTypeContext nerType() throws RecognitionException {
		NerTypeContext _localctx = new NerTypeContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_nerType);
		try {
			setState(92);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
			case IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(90);
				identifier();
				}
				break;
			case WILDCARD:
				enterOuterAlt(_localctx, 2);
				{
				setState(91);
				match(WILDCARD);
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
	public static class NerTargetContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public NerTargetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nerTarget; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterNerTarget(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitNerTarget(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitNerTarget(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NerTargetContext nerTarget() throws RecognitionException {
		NerTargetContext _localctx = new NerTargetContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_nerTarget);
		try {
			setState(96);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
			case IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(94);
				identifier();
				}
				break;
			case VARIABLE:
				enterOuterAlt(_localctx, 2);
				{
				setState(95);
				variable();
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
	public static class TemporalExpressionContext extends ParserRuleContext {
		public TemporalSpecContext dateSpec;
		public VariableContext dateVar;
		public TerminalNode TEMPORAL() { return getToken(QueryLangParser.TEMPORAL, 0); }
		public TemporalSpecContext temporalSpec() {
			return getRuleContext(TemporalSpecContext.class,0);
		}
		public TerminalNode DATE() { return getToken(QueryLangParser.DATE, 0); }
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public TemporalOperatorContext temporalOperator() {
			return getRuleContext(TemporalOperatorContext.class,0);
		}
		public DateValueContext dateValue() {
			return getRuleContext(DateValueContext.class,0);
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
		enterRule(_localctx, 16, RULE_temporalExpression);
		try {
			setState(115);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(98);
				match(TEMPORAL);
				setState(99);
				match(T__1);
				setState(100);
				((TemporalExpressionContext)_localctx).dateSpec = temporalSpec();
				setState(101);
				match(T__2);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(103);
				match(DATE);
				setState(104);
				match(T__1);
				setState(105);
				((TemporalExpressionContext)_localctx).dateVar = variable();
				setState(106);
				match(T__2);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(108);
				match(DATE);
				setState(109);
				match(T__1);
				setState(110);
				((TemporalExpressionContext)_localctx).dateVar = variable();
				setState(111);
				match(T__2);
				setState(112);
				temporalOperator();
				setState(113);
				dateValue();
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
	public static class TemporalSpecContext extends ParserRuleContext {
		public TemporalValueContext date;
		public TemporalValueContext start;
		public TemporalValueContext end;
		public Token range;
		public TerminalNode BEFORE() { return getToken(QueryLangParser.BEFORE, 0); }
		public List<TemporalValueContext> temporalValue() {
			return getRuleContexts(TemporalValueContext.class);
		}
		public TemporalValueContext temporalValue(int i) {
			return getRuleContext(TemporalValueContext.class,i);
		}
		public TerminalNode AFTER() { return getToken(QueryLangParser.AFTER, 0); }
		public TerminalNode BETWEEN() { return getToken(QueryLangParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(QueryLangParser.AND, 0); }
		public TerminalNode NEAR() { return getToken(QueryLangParser.NEAR, 0); }
		public TerminalNode STRING() { return getToken(QueryLangParser.STRING, 0); }
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
		enterRule(_localctx, 18, RULE_temporalSpec);
		try {
			setState(131);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case BEFORE:
				enterOuterAlt(_localctx, 1);
				{
				setState(117);
				match(BEFORE);
				setState(118);
				((TemporalSpecContext)_localctx).date = temporalValue();
				}
				break;
			case AFTER:
				enterOuterAlt(_localctx, 2);
				{
				setState(119);
				match(AFTER);
				setState(120);
				((TemporalSpecContext)_localctx).date = temporalValue();
				}
				break;
			case BETWEEN:
				enterOuterAlt(_localctx, 3);
				{
				setState(121);
				match(BETWEEN);
				setState(122);
				((TemporalSpecContext)_localctx).start = temporalValue();
				setState(123);
				match(AND);
				setState(124);
				((TemporalSpecContext)_localctx).end = temporalValue();
				}
				break;
			case NEAR:
				enterOuterAlt(_localctx, 4);
				{
				setState(126);
				match(NEAR);
				setState(127);
				((TemporalSpecContext)_localctx).date = temporalValue();
				setState(128);
				match(T__3);
				setState(129);
				((TemporalSpecContext)_localctx).range = match(STRING);
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
	public static class TemporalValueContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(QueryLangParser.STRING, 0); }
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public TemporalValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_temporalValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterTemporalValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitTemporalValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitTemporalValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TemporalValueContext temporalValue() throws RecognitionException {
		TemporalValueContext _localctx = new TemporalValueContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_temporalValue);
		try {
			setState(135);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
				enterOuterAlt(_localctx, 1);
				{
				setState(133);
				match(STRING);
				}
				break;
			case VARIABLE:
				enterOuterAlt(_localctx, 2);
				{
				setState(134);
				variable();
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
	public static class TemporalOperatorContext extends ParserRuleContext {
		public TerminalNode BEFORE() { return getToken(QueryLangParser.BEFORE, 0); }
		public TerminalNode AFTER() { return getToken(QueryLangParser.AFTER, 0); }
		public TerminalNode BETWEEN() { return getToken(QueryLangParser.BETWEEN, 0); }
		public TerminalNode NEAR() { return getToken(QueryLangParser.NEAR, 0); }
		public TemporalOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_temporalOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterTemporalOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitTemporalOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitTemporalOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TemporalOperatorContext temporalOperator() throws RecognitionException {
		TemporalOperatorContext _localctx = new TemporalOperatorContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_temporalOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(137);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 15728640L) != 0)) ) {
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
	public static class DateValueContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(QueryLangParser.STRING, 0); }
		public SubQueryContext subQuery() {
			return getRuleContext(SubQueryContext.class,0);
		}
		public DateValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dateValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterDateValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitDateValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitDateValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DateValueContext dateValue() throws RecognitionException {
		DateValueContext _localctx = new DateValueContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_dateValue);
		try {
			setState(141);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
				enterOuterAlt(_localctx, 1);
				{
				setState(139);
				match(STRING);
				}
				break;
			case T__4:
				enterOuterAlt(_localctx, 2);
				{
				setState(140);
				subQuery();
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
		public DepComponentContext governor;
		public IdentifierContext relation;
		public DepComponentContext dependent;
		public TerminalNode DEPENDENCY() { return getToken(QueryLangParser.DEPENDENCY, 0); }
		public List<DepComponentContext> depComponent() {
			return getRuleContexts(DepComponentContext.class);
		}
		public DepComponentContext depComponent(int i) {
			return getRuleContext(DepComponentContext.class,i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
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
		enterRule(_localctx, 26, RULE_dependencyExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(143);
			match(DEPENDENCY);
			setState(144);
			match(T__1);
			setState(145);
			((DependencyExpressionContext)_localctx).governor = depComponent();
			setState(146);
			match(T__3);
			setState(147);
			((DependencyExpressionContext)_localctx).relation = identifier();
			setState(148);
			match(T__3);
			setState(149);
			((DependencyExpressionContext)_localctx).dependent = depComponent();
			setState(150);
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
	public static class DepComponentContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public DepComponentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_depComponent; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterDepComponent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitDepComponent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitDepComponent(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DepComponentContext depComponent() throws RecognitionException {
		DepComponentContext _localctx = new DepComponentContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_depComponent);
		try {
			setState(154);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
			case IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(152);
				identifier();
				}
				break;
			case VARIABLE:
				enterOuterAlt(_localctx, 2);
				{
				setState(153);
				variable();
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
	public static class VariableContext extends ParserRuleContext {
		public Token name;
		public TerminalNode VARIABLE() { return getToken(QueryLangParser.VARIABLE, 0); }
		public TerminalNode IDENTIFIER() { return getToken(QueryLangParser.IDENTIFIER, 0); }
		public VariableModifierContext variableModifier() {
			return getRuleContext(VariableModifierContext.class,0);
		}
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
		enterRule(_localctx, 30, RULE_variable);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(156);
			match(VARIABLE);
			setState(157);
			((VariableContext)_localctx).name = match(IDENTIFIER);
			setState(159);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WILDCARD || _la==OPTIONAL) {
				{
				setState(158);
				variableModifier();
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
	public static class VariableModifierContext extends ParserRuleContext {
		public TerminalNode WILDCARD() { return getToken(QueryLangParser.WILDCARD, 0); }
		public TerminalNode OPTIONAL() { return getToken(QueryLangParser.OPTIONAL, 0); }
		public VariableModifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_variableModifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterVariableModifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitVariableModifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitVariableModifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final VariableModifierContext variableModifier() throws RecognitionException {
		VariableModifierContext _localctx = new VariableModifierContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_variableModifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(161);
			_la = _input.LA(1);
			if ( !(_la==WILDCARD || _la==OPTIONAL) ) {
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
	public static class SubQueryContext extends ParserRuleContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterSubQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitSubQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitSubQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubQueryContext subQuery() throws RecognitionException {
		SubQueryContext _localctx = new SubQueryContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_subQuery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(163);
			match(T__4);
			setState(164);
			query();
			setState(165);
			match(T__5);
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
		enterRule(_localctx, 36, RULE_orderByClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(167);
			match(ORDER);
			setState(168);
			match(BY);
			setState(169);
			orderSpec();
			setState(174);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(170);
				match(T__3);
				setState(171);
				orderSpec();
				}
				}
				setState(176);
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
		enterRule(_localctx, 38, RULE_orderSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(177);
			((OrderSpecContext)_localctx).field = identifier();
			setState(179);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(178);
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
		enterRule(_localctx, 40, RULE_limitClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(181);
			match(LIMIT);
			setState(182);
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
		"\u0004\u0001 \u00b9\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
		"\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
		"\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
		"\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0001\u0000\u0001\u0000"+
		"\u0001\u0000\u0001\u0000\u0001\u0000\u0003\u00000\b\u0000\u0001\u0000"+
		"\u0003\u00003\b\u0000\u0001\u0000\u0003\u00006\b\u0000\u0001\u0001\u0001"+
		"\u0001\u0001\u0002\u0001\u0002\u0004\u0002<\b\u0002\u000b\u0002\f\u0002"+
		"=\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0004\u0003G\b\u0003\u000b\u0003\f\u0003H\u0001\u0003\u0001"+
		"\u0003\u0003\u0003M\b\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
		"\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0006\u0001\u0006\u0003\u0006]\b"+
		"\u0006\u0001\u0007\u0001\u0007\u0003\u0007a\b\u0007\u0001\b\u0001\b\u0001"+
		"\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001"+
		"\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0003\bt\b\b\u0001\t\u0001"+
		"\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001"+
		"\t\u0001\t\u0001\t\u0001\t\u0003\t\u0084\b\t\u0001\n\u0001\n\u0003\n\u0088"+
		"\b\n\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0003\f\u008e\b\f\u0001\r"+
		"\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001"+
		"\u000e\u0001\u000e\u0003\u000e\u009b\b\u000e\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0003\u000f\u00a0\b\u000f\u0001\u0010\u0001\u0010\u0001\u0011\u0001"+
		"\u0011\u0001\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0001"+
		"\u0012\u0001\u0012\u0005\u0012\u00ad\b\u0012\n\u0012\f\u0012\u00b0\t\u0012"+
		"\u0001\u0013\u0001\u0013\u0003\u0013\u00b4\b\u0013\u0001\u0014\u0001\u0014"+
		"\u0001\u0014\u0001\u0014\u0000\u0000\u0015\u0000\u0002\u0004\u0006\b\n"+
		"\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e \"$&(\u0000\u0004"+
		"\u0001\u0000\u001c\u001d\u0001\u0000\u0014\u0017\u0001\u0000\u001a\u001b"+
		"\u0001\u0000\u0011\u0012\u00ba\u0000*\u0001\u0000\u0000\u0000\u00027\u0001"+
		"\u0000\u0000\u0000\u00049\u0001\u0000\u0000\u0000\u0006L\u0001\u0000\u0000"+
		"\u0000\bN\u0001\u0000\u0000\u0000\nS\u0001\u0000\u0000\u0000\f\\\u0001"+
		"\u0000\u0000\u0000\u000e`\u0001\u0000\u0000\u0000\u0010s\u0001\u0000\u0000"+
		"\u0000\u0012\u0083\u0001\u0000\u0000\u0000\u0014\u0087\u0001\u0000\u0000"+
		"\u0000\u0016\u0089\u0001\u0000\u0000\u0000\u0018\u008d\u0001\u0000\u0000"+
		"\u0000\u001a\u008f\u0001\u0000\u0000\u0000\u001c\u009a\u0001\u0000\u0000"+
		"\u0000\u001e\u009c\u0001\u0000\u0000\u0000 \u00a1\u0001\u0000\u0000\u0000"+
		"\"\u00a3\u0001\u0000\u0000\u0000$\u00a7\u0001\u0000\u0000\u0000&\u00b1"+
		"\u0001\u0000\u0000\u0000(\u00b5\u0001\u0000\u0000\u0000*+\u0005\u0007"+
		"\u0000\u0000+,\u0005\u0001\u0000\u0000,-\u0005\b\u0000\u0000-/\u0003\u0002"+
		"\u0001\u0000.0\u0003\u0004\u0002\u0000/.\u0001\u0000\u0000\u0000/0\u0001"+
		"\u0000\u0000\u000002\u0001\u0000\u0000\u000013\u0003$\u0012\u000021\u0001"+
		"\u0000\u0000\u000023\u0001\u0000\u0000\u000035\u0001\u0000\u0000\u0000"+
		"46\u0003(\u0014\u000054\u0001\u0000\u0000\u000056\u0001\u0000\u0000\u0000"+
		"6\u0001\u0001\u0000\u0000\u000078\u0007\u0000\u0000\u00008\u0003\u0001"+
		"\u0000\u0000\u00009;\u0005\t\u0000\u0000:<\u0003\u0006\u0003\u0000;:\u0001"+
		"\u0000\u0000\u0000<=\u0001\u0000\u0000\u0000=;\u0001\u0000\u0000\u0000"+
		"=>\u0001\u0000\u0000\u0000>\u0005\u0001\u0000\u0000\u0000?M\u0003\b\u0004"+
		"\u0000@M\u0003\n\u0005\u0000AM\u0003\u0010\b\u0000BM\u0003\u001a\r\u0000"+
		"CM\u0003\"\u0011\u0000DF\u0005\u0002\u0000\u0000EG\u0003\u0006\u0003\u0000"+
		"FE\u0001\u0000\u0000\u0000GH\u0001\u0000\u0000\u0000HF\u0001\u0000\u0000"+
		"\u0000HI\u0001\u0000\u0000\u0000IJ\u0001\u0000\u0000\u0000JK\u0005\u0003"+
		"\u0000\u0000KM\u0001\u0000\u0000\u0000L?\u0001\u0000\u0000\u0000L@\u0001"+
		"\u0000\u0000\u0000LA\u0001\u0000\u0000\u0000LB\u0001\u0000\u0000\u0000"+
		"LC\u0001\u0000\u0000\u0000LD\u0001\u0000\u0000\u0000M\u0007\u0001\u0000"+
		"\u0000\u0000NO\u0005\n\u0000\u0000OP\u0005\u0002\u0000\u0000PQ\u0005\u001c"+
		"\u0000\u0000QR\u0005\u0003\u0000\u0000R\t\u0001\u0000\u0000\u0000ST\u0005"+
		"\u000b\u0000\u0000TU\u0005\u0002\u0000\u0000UV\u0003\f\u0006\u0000VW\u0005"+
		"\u0004\u0000\u0000WX\u0003\u000e\u0007\u0000XY\u0005\u0003\u0000\u0000"+
		"Y\u000b\u0001\u0000\u0000\u0000Z]\u0003\u0002\u0001\u0000[]\u0005\u001a"+
		"\u0000\u0000\\Z\u0001\u0000\u0000\u0000\\[\u0001\u0000\u0000\u0000]\r"+
		"\u0001\u0000\u0000\u0000^a\u0003\u0002\u0001\u0000_a\u0003\u001e\u000f"+
		"\u0000`^\u0001\u0000\u0000\u0000`_\u0001\u0000\u0000\u0000a\u000f\u0001"+
		"\u0000\u0000\u0000bc\u0005\f\u0000\u0000cd\u0005\u0002\u0000\u0000de\u0003"+
		"\u0012\t\u0000ef\u0005\u0003\u0000\u0000ft\u0001\u0000\u0000\u0000gh\u0005"+
		"\r\u0000\u0000hi\u0005\u0002\u0000\u0000ij\u0003\u001e\u000f\u0000jk\u0005"+
		"\u0003\u0000\u0000kt\u0001\u0000\u0000\u0000lm\u0005\r\u0000\u0000mn\u0005"+
		"\u0002\u0000\u0000no\u0003\u001e\u000f\u0000op\u0005\u0003\u0000\u0000"+
		"pq\u0003\u0016\u000b\u0000qr\u0003\u0018\f\u0000rt\u0001\u0000\u0000\u0000"+
		"sb\u0001\u0000\u0000\u0000sg\u0001\u0000\u0000\u0000sl\u0001\u0000\u0000"+
		"\u0000t\u0011\u0001\u0000\u0000\u0000uv\u0005\u0014\u0000\u0000v\u0084"+
		"\u0003\u0014\n\u0000wx\u0005\u0015\u0000\u0000x\u0084\u0003\u0014\n\u0000"+
		"yz\u0005\u0016\u0000\u0000z{\u0003\u0014\n\u0000{|\u0005\u0018\u0000\u0000"+
		"|}\u0003\u0014\n\u0000}\u0084\u0001\u0000\u0000\u0000~\u007f\u0005\u0017"+
		"\u0000\u0000\u007f\u0080\u0003\u0014\n\u0000\u0080\u0081\u0005\u0004\u0000"+
		"\u0000\u0081\u0082\u0005\u001c\u0000\u0000\u0082\u0084\u0001\u0000\u0000"+
		"\u0000\u0083u\u0001\u0000\u0000\u0000\u0083w\u0001\u0000\u0000\u0000\u0083"+
		"y\u0001\u0000\u0000\u0000\u0083~\u0001\u0000\u0000\u0000\u0084\u0013\u0001"+
		"\u0000\u0000\u0000\u0085\u0088\u0005\u001c\u0000\u0000\u0086\u0088\u0003"+
		"\u001e\u000f\u0000\u0087\u0085\u0001\u0000\u0000\u0000\u0087\u0086\u0001"+
		"\u0000\u0000\u0000\u0088\u0015\u0001\u0000\u0000\u0000\u0089\u008a\u0007"+
		"\u0001\u0000\u0000\u008a\u0017\u0001\u0000\u0000\u0000\u008b\u008e\u0005"+
		"\u001c\u0000\u0000\u008c\u008e\u0003\"\u0011\u0000\u008d\u008b\u0001\u0000"+
		"\u0000\u0000\u008d\u008c\u0001\u0000\u0000\u0000\u008e\u0019\u0001\u0000"+
		"\u0000\u0000\u008f\u0090\u0005\u000e\u0000\u0000\u0090\u0091\u0005\u0002"+
		"\u0000\u0000\u0091\u0092\u0003\u001c\u000e\u0000\u0092\u0093\u0005\u0004"+
		"\u0000\u0000\u0093\u0094\u0003\u0002\u0001\u0000\u0094\u0095\u0005\u0004"+
		"\u0000\u0000\u0095\u0096\u0003\u001c\u000e\u0000\u0096\u0097\u0005\u0003"+
		"\u0000\u0000\u0097\u001b\u0001\u0000\u0000\u0000\u0098\u009b\u0003\u0002"+
		"\u0001\u0000\u0099\u009b\u0003\u001e\u000f\u0000\u009a\u0098\u0001\u0000"+
		"\u0000\u0000\u009a\u0099\u0001\u0000\u0000\u0000\u009b\u001d\u0001\u0000"+
		"\u0000\u0000\u009c\u009d\u0005\u0019\u0000\u0000\u009d\u009f\u0005\u001d"+
		"\u0000\u0000\u009e\u00a0\u0003 \u0010\u0000\u009f\u009e\u0001\u0000\u0000"+
		"\u0000\u009f\u00a0\u0001\u0000\u0000\u0000\u00a0\u001f\u0001\u0000\u0000"+
		"\u0000\u00a1\u00a2\u0007\u0002\u0000\u0000\u00a2!\u0001\u0000\u0000\u0000"+
		"\u00a3\u00a4\u0005\u0005\u0000\u0000\u00a4\u00a5\u0003\u0000\u0000\u0000"+
		"\u00a5\u00a6\u0005\u0006\u0000\u0000\u00a6#\u0001\u0000\u0000\u0000\u00a7"+
		"\u00a8\u0005\u000f\u0000\u0000\u00a8\u00a9\u0005\u0010\u0000\u0000\u00a9"+
		"\u00ae\u0003&\u0013\u0000\u00aa\u00ab\u0005\u0004\u0000\u0000\u00ab\u00ad"+
		"\u0003&\u0013\u0000\u00ac\u00aa\u0001\u0000\u0000\u0000\u00ad\u00b0\u0001"+
		"\u0000\u0000\u0000\u00ae\u00ac\u0001\u0000\u0000\u0000\u00ae\u00af\u0001"+
		"\u0000\u0000\u0000\u00af%\u0001\u0000\u0000\u0000\u00b0\u00ae\u0001\u0000"+
		"\u0000\u0000\u00b1\u00b3\u0003\u0002\u0001\u0000\u00b2\u00b4\u0007\u0003"+
		"\u0000\u0000\u00b3\u00b2\u0001\u0000\u0000\u0000\u00b3\u00b4\u0001\u0000"+
		"\u0000\u0000\u00b4\'\u0001\u0000\u0000\u0000\u00b5\u00b6\u0005\u0013\u0000"+
		"\u0000\u00b6\u00b7\u0005\u001e\u0000\u0000\u00b7)\u0001\u0000\u0000\u0000"+
		"\u0010/25=HL\\`s\u0083\u0087\u008d\u009a\u009f\u00ae\u00b3";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}