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
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, SELECT=13, FROM=14, WHERE=15, AND=16, CONTAINS=17, 
		NER=18, DATE=19, DEPENDS=20, ORDER=21, BY=22, ASC=23, DESC=24, LIMIT=25, 
		NEAR=26, WILDCARD=27, STRING=28, IDENTIFIER=29, NUMBER=30, WS=31, COMMENT=32;
	public static final int
		RULE_query = 0, RULE_identifier = 1, RULE_whereClause = 2, RULE_conditionList = 3, 
		RULE_singleCondition = 4, RULE_nerExpression = 5, RULE_entityType = 6, 
		RULE_entityTarget = 7, RULE_containsExpression = 8, RULE_dateExpression = 9, 
		RULE_dateOperator = 10, RULE_dateValue = 11, RULE_rangeSpec = 12, RULE_dependsExpression = 13, 
		RULE_governor = 14, RULE_dependent = 15, RULE_relation = 16, RULE_variable = 17, 
		RULE_subQuery = 18, RULE_orderByClause = 19, RULE_orderSpec = 20, RULE_limitClause = 21;
	private static String[] makeRuleNames() {
		return new String[] {
			"query", "identifier", "whereClause", "conditionList", "singleCondition", 
			"nerExpression", "entityType", "entityTarget", "containsExpression", 
			"dateExpression", "dateOperator", "dateValue", "rangeSpec", "dependsExpression", 
			"governor", "dependent", "relation", "variable", "subQuery", "orderByClause", 
			"orderSpec", "limitClause"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'documents'", "'('", "')'", "','", "'<'", "'>'", "'<='", "'>='", 
			"'=='", "'?'", "'{'", "'}'", "'SELECT'", "'FROM'", "'WHERE'", "'AND'", 
			"'CONTAINS'", "'NER'", "'DATE'", "'DEPENDS'", "'ORDER'", "'BY'", "'ASC'", 
			"'DESC'", "'LIMIT'", "'NEAR'", "'*'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, "SELECT", "FROM", "WHERE", "AND", "CONTAINS", "NER", "DATE", "DEPENDS", 
			"ORDER", "BY", "ASC", "DESC", "LIMIT", "NEAR", "WILDCARD", "STRING", 
			"IDENTIFIER", "NUMBER", "WS", "COMMENT"
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
		public TerminalNode EOF() { return getToken(QueryLangParser.EOF, 0); }
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
			setState(44);
			match(SELECT);
			setState(45);
			match(T__0);
			setState(46);
			match(FROM);
			setState(47);
			((QueryContext)_localctx).source = identifier();
			setState(49);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(48);
				whereClause();
				}
			}

			setState(52);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(51);
				orderByClause();
				}
			}

			setState(55);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(54);
				limitClause();
				}
			}

			setState(57);
			match(EOF);
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
			setState(59);
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
		public ConditionListContext conditions;
		public TerminalNode WHERE() { return getToken(QueryLangParser.WHERE, 0); }
		public ConditionListContext conditionList() {
			return getRuleContext(ConditionListContext.class,0);
		}
		public TerminalNode EOF() { return getToken(QueryLangParser.EOF, 0); }
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
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(61);
			match(WHERE);
			setState(62);
			((WhereClauseContext)_localctx).conditions = conditionList();
			setState(64);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
			case 1:
				{
				setState(63);
				match(EOF);
				}
				break;
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
	public static class ConditionListContext extends ParserRuleContext {
		public SingleConditionContext first;
		public SingleConditionContext singleCondition;
		public List<SingleConditionContext> next = new ArrayList<SingleConditionContext>();
		public List<SingleConditionContext> singleCondition() {
			return getRuleContexts(SingleConditionContext.class);
		}
		public SingleConditionContext singleCondition(int i) {
			return getRuleContext(SingleConditionContext.class,i);
		}
		public List<TerminalNode> AND() { return getTokens(QueryLangParser.AND); }
		public TerminalNode AND(int i) {
			return getToken(QueryLangParser.AND, i);
		}
		public ConditionListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_conditionList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterConditionList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitConditionList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitConditionList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConditionListContext conditionList() throws RecognitionException {
		ConditionListContext _localctx = new ConditionListContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_conditionList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(66);
			((ConditionListContext)_localctx).first = singleCondition();
			setState(71);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==AND) {
				{
				{
				setState(67);
				match(AND);
				setState(68);
				((ConditionListContext)_localctx).singleCondition = singleCondition();
				((ConditionListContext)_localctx).next.add(((ConditionListContext)_localctx).singleCondition);
				}
				}
				setState(73);
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
	public static class SingleConditionContext extends ParserRuleContext {
		public ConditionListContext nested;
		public NerExpressionContext nerExpression() {
			return getRuleContext(NerExpressionContext.class,0);
		}
		public ContainsExpressionContext containsExpression() {
			return getRuleContext(ContainsExpressionContext.class,0);
		}
		public DateExpressionContext dateExpression() {
			return getRuleContext(DateExpressionContext.class,0);
		}
		public DependsExpressionContext dependsExpression() {
			return getRuleContext(DependsExpressionContext.class,0);
		}
		public SubQueryContext subQuery() {
			return getRuleContext(SubQueryContext.class,0);
		}
		public ConditionListContext conditionList() {
			return getRuleContext(ConditionListContext.class,0);
		}
		public SingleConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleCondition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterSingleCondition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitSingleCondition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitSingleCondition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleConditionContext singleCondition() throws RecognitionException {
		SingleConditionContext _localctx = new SingleConditionContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_singleCondition);
		try {
			setState(83);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NER:
				enterOuterAlt(_localctx, 1);
				{
				setState(74);
				nerExpression();
				}
				break;
			case CONTAINS:
				enterOuterAlt(_localctx, 2);
				{
				setState(75);
				containsExpression();
				}
				break;
			case DATE:
				enterOuterAlt(_localctx, 3);
				{
				setState(76);
				dateExpression();
				}
				break;
			case DEPENDS:
				enterOuterAlt(_localctx, 4);
				{
				setState(77);
				dependsExpression();
				}
				break;
			case T__10:
				enterOuterAlt(_localctx, 5);
				{
				setState(78);
				subQuery();
				}
				break;
			case T__1:
				enterOuterAlt(_localctx, 6);
				{
				setState(79);
				match(T__1);
				setState(80);
				((SingleConditionContext)_localctx).nested = conditionList();
				setState(81);
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
	public static class NerExpressionContext extends ParserRuleContext {
		public EntityTypeContext type;
		public EntityTargetContext target;
		public TerminalNode NER() { return getToken(QueryLangParser.NER, 0); }
		public EntityTypeContext entityType() {
			return getRuleContext(EntityTypeContext.class,0);
		}
		public EntityTargetContext entityTarget() {
			return getRuleContext(EntityTargetContext.class,0);
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
			setState(85);
			match(NER);
			setState(86);
			match(T__1);
			setState(87);
			((NerExpressionContext)_localctx).type = entityType();
			setState(88);
			match(T__3);
			setState(89);
			((NerExpressionContext)_localctx).target = entityTarget();
			setState(90);
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
	public static class EntityTypeContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(QueryLangParser.STRING, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode WILDCARD() { return getToken(QueryLangParser.WILDCARD, 0); }
		public EntityTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_entityType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterEntityType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitEntityType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitEntityType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EntityTypeContext entityType() throws RecognitionException {
		EntityTypeContext _localctx = new EntityTypeContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_entityType);
		try {
			setState(95);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(92);
				match(STRING);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(93);
				identifier();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(94);
				match(WILDCARD);
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
	public static class EntityTargetContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(QueryLangParser.STRING, 0); }
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public EntityTargetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_entityTarget; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterEntityTarget(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitEntityTarget(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitEntityTarget(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EntityTargetContext entityTarget() throws RecognitionException {
		EntityTargetContext _localctx = new EntityTargetContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_entityTarget);
		try {
			setState(99);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
				enterOuterAlt(_localctx, 1);
				{
				setState(97);
				match(STRING);
				}
				break;
			case T__9:
				enterOuterAlt(_localctx, 2);
				{
				setState(98);
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
	public static class ContainsExpressionContext extends ParserRuleContext {
		public Token text;
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
		enterRule(_localctx, 16, RULE_containsExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(101);
			match(CONTAINS);
			setState(102);
			match(T__1);
			setState(103);
			((ContainsExpressionContext)_localctx).text = match(STRING);
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
	public static class DateExpressionContext extends ParserRuleContext {
		public VariableContext dateVar;
		public DateOperatorContext dateOp;
		public DateValueContext dateCompareValue;
		public Token dateString;
		public RangeSpecContext dateRange;
		public TerminalNode DATE() { return getToken(QueryLangParser.DATE, 0); }
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public DateOperatorContext dateOperator() {
			return getRuleContext(DateOperatorContext.class,0);
		}
		public DateValueContext dateValue() {
			return getRuleContext(DateValueContext.class,0);
		}
		public TerminalNode STRING() { return getToken(QueryLangParser.STRING, 0); }
		public TerminalNode NEAR() { return getToken(QueryLangParser.NEAR, 0); }
		public RangeSpecContext rangeSpec() {
			return getRuleContext(RangeSpecContext.class,0);
		}
		public DateExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dateExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterDateExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitDateExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitDateExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DateExpressionContext dateExpression() throws RecognitionException {
		DateExpressionContext _localctx = new DateExpressionContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_dateExpression);
		try {
			setState(131);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(106);
				match(DATE);
				setState(107);
				match(T__1);
				setState(108);
				((DateExpressionContext)_localctx).dateVar = variable();
				setState(109);
				match(T__2);
				setState(110);
				((DateExpressionContext)_localctx).dateOp = dateOperator();
				setState(111);
				((DateExpressionContext)_localctx).dateCompareValue = dateValue();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(113);
				match(DATE);
				setState(114);
				match(T__1);
				setState(115);
				((DateExpressionContext)_localctx).dateVar = variable();
				setState(116);
				match(T__2);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(118);
				match(DATE);
				setState(119);
				((DateExpressionContext)_localctx).dateString = match(STRING);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(120);
				match(DATE);
				setState(121);
				match(T__1);
				setState(122);
				((DateExpressionContext)_localctx).dateVar = variable();
				setState(123);
				match(T__2);
				setState(124);
				match(NEAR);
				setState(125);
				match(T__1);
				setState(126);
				((DateExpressionContext)_localctx).dateCompareValue = dateValue();
				setState(127);
				match(T__3);
				setState(128);
				((DateExpressionContext)_localctx).dateRange = rangeSpec();
				setState(129);
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
	public static class DateOperatorContext extends ParserRuleContext {
		public DateOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dateOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterDateOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitDateOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitDateOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DateOperatorContext dateOperator() throws RecognitionException {
		DateOperatorContext _localctx = new DateOperatorContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_dateOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(133);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 992L) != 0)) ) {
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
		enterRule(_localctx, 22, RULE_dateValue);
		try {
			setState(137);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
				enterOuterAlt(_localctx, 1);
				{
				setState(135);
				match(STRING);
				}
				break;
			case T__10:
				enterOuterAlt(_localctx, 2);
				{
				setState(136);
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
	public static class RangeSpecContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(QueryLangParser.STRING, 0); }
		public RangeSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterRangeSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitRangeSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitRangeSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeSpecContext rangeSpec() throws RecognitionException {
		RangeSpecContext _localctx = new RangeSpecContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_rangeSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(139);
			match(STRING);
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
	public static class DependsExpressionContext extends ParserRuleContext {
		public GovernorContext gov;
		public RelationContext rel;
		public DependentContext dep;
		public TerminalNode DEPENDS() { return getToken(QueryLangParser.DEPENDS, 0); }
		public GovernorContext governor() {
			return getRuleContext(GovernorContext.class,0);
		}
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public DependentContext dependent() {
			return getRuleContext(DependentContext.class,0);
		}
		public DependsExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dependsExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterDependsExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitDependsExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitDependsExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DependsExpressionContext dependsExpression() throws RecognitionException {
		DependsExpressionContext _localctx = new DependsExpressionContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_dependsExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(141);
			match(DEPENDS);
			setState(142);
			match(T__1);
			setState(143);
			((DependsExpressionContext)_localctx).gov = governor();
			setState(144);
			match(T__3);
			setState(145);
			((DependsExpressionContext)_localctx).rel = relation();
			setState(146);
			match(T__3);
			setState(147);
			((DependsExpressionContext)_localctx).dep = dependent();
			setState(148);
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
	public static class GovernorContext extends ParserRuleContext {
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public TerminalNode STRING() { return getToken(QueryLangParser.STRING, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public GovernorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_governor; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterGovernor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitGovernor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitGovernor(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GovernorContext governor() throws RecognitionException {
		GovernorContext _localctx = new GovernorContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_governor);
		try {
			setState(153);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(150);
				variable();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(151);
				match(STRING);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(152);
				identifier();
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
	public static class DependentContext extends ParserRuleContext {
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public TerminalNode STRING() { return getToken(QueryLangParser.STRING, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DependentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dependent; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterDependent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitDependent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitDependent(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DependentContext dependent() throws RecognitionException {
		DependentContext _localctx = new DependentContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_dependent);
		try {
			setState(158);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(155);
				variable();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(156);
				match(STRING);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(157);
				identifier();
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
	public static class RelationContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(QueryLangParser.STRING, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).enterRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof QueryLangListener ) ((QueryLangListener)listener).exitRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof QueryLangVisitor ) return ((QueryLangVisitor<? extends T>)visitor).visitRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		RelationContext _localctx = new RelationContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_relation);
		try {
			setState(162);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(160);
				match(STRING);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(161);
				identifier();
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
	public static class VariableContext extends ParserRuleContext {
		public Token name;
		public Token wild;
		public TerminalNode IDENTIFIER() { return getToken(QueryLangParser.IDENTIFIER, 0); }
		public TerminalNode WILDCARD() { return getToken(QueryLangParser.WILDCARD, 0); }
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
		enterRule(_localctx, 34, RULE_variable);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(164);
			match(T__9);
			setState(165);
			((VariableContext)_localctx).name = match(IDENTIFIER);
			setState(167);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WILDCARD) {
				{
				setState(166);
				((VariableContext)_localctx).wild = match(WILDCARD);
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
	public static class SubQueryContext extends ParserRuleContext {
		public QueryContext nested;
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
		enterRule(_localctx, 36, RULE_subQuery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(169);
			match(T__10);
			setState(170);
			((SubQueryContext)_localctx).nested = query();
			setState(171);
			match(T__11);
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
		public OrderSpecContext first;
		public OrderSpecContext orderSpec;
		public List<OrderSpecContext> next = new ArrayList<OrderSpecContext>();
		public TerminalNode ORDER() { return getToken(QueryLangParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(QueryLangParser.BY, 0); }
		public List<OrderSpecContext> orderSpec() {
			return getRuleContexts(OrderSpecContext.class);
		}
		public OrderSpecContext orderSpec(int i) {
			return getRuleContext(OrderSpecContext.class,i);
		}
		public TerminalNode EOF() { return getToken(QueryLangParser.EOF, 0); }
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
		enterRule(_localctx, 38, RULE_orderByClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(173);
			match(ORDER);
			setState(174);
			match(BY);
			setState(175);
			((OrderByClauseContext)_localctx).first = orderSpec();
			setState(180);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(176);
				match(T__3);
				setState(177);
				((OrderByClauseContext)_localctx).orderSpec = orderSpec();
				((OrderByClauseContext)_localctx).next.add(((OrderByClauseContext)_localctx).orderSpec);
				}
				}
				setState(182);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(184);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
			case 1:
				{
				setState(183);
				match(EOF);
				}
				break;
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
		public Token dir;
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
		enterRule(_localctx, 40, RULE_orderSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(186);
			((OrderSpecContext)_localctx).field = identifier();
			setState(188);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(187);
				((OrderSpecContext)_localctx).dir = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((OrderSpecContext)_localctx).dir = (Token)_errHandler.recoverInline(this);
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
		public TerminalNode EOF() { return getToken(QueryLangParser.EOF, 0); }
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
		enterRule(_localctx, 42, RULE_limitClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(190);
			match(LIMIT);
			setState(191);
			((LimitClauseContext)_localctx).count = match(NUMBER);
			setState(193);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
			case 1:
				{
				setState(192);
				match(EOF);
				}
				break;
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

	public static final String _serializedATN =
		"\u0004\u0001 \u00c4\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
		"\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
		"\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
		"\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"+
		"\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0003\u0000"+
		"2\b\u0000\u0001\u0000\u0003\u00005\b\u0000\u0001\u0000\u0003\u00008\b"+
		"\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0003\u0002A\b\u0002\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0005\u0003F\b\u0003\n\u0003\f\u0003I\t\u0003\u0001\u0004\u0001"+
		"\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
		"\u0004\u0001\u0004\u0003\u0004T\b\u0004\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0003\u0006`\b\u0006\u0001\u0007\u0001\u0007\u0003"+
		"\u0007d\b\u0007\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\t\u0001"+
		"\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001"+
		"\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001"+
		"\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0003\t\u0084\b\t\u0001\n\u0001"+
		"\n\u0001\u000b\u0001\u000b\u0003\u000b\u008a\b\u000b\u0001\f\u0001\f\u0001"+
		"\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001"+
		"\u000e\u0001\u000e\u0001\u000e\u0003\u000e\u009a\b\u000e\u0001\u000f\u0001"+
		"\u000f\u0001\u000f\u0003\u000f\u009f\b\u000f\u0001\u0010\u0001\u0010\u0003"+
		"\u0010\u00a3\b\u0010\u0001\u0011\u0001\u0011\u0001\u0011\u0003\u0011\u00a8"+
		"\b\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0013\u0001"+
		"\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0005\u0013\u00b3\b\u0013\n"+
		"\u0013\f\u0013\u00b6\t\u0013\u0001\u0013\u0003\u0013\u00b9\b\u0013\u0001"+
		"\u0014\u0001\u0014\u0003\u0014\u00bd\b\u0014\u0001\u0015\u0001\u0015\u0001"+
		"\u0015\u0003\u0015\u00c2\b\u0015\u0001\u0015\u0000\u0000\u0016\u0000\u0002"+
		"\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e"+
		" \"$&(*\u0000\u0003\u0001\u0000\u001c\u001d\u0001\u0000\u0005\t\u0001"+
		"\u0000\u0017\u0018\u00c8\u0000,\u0001\u0000\u0000\u0000\u0002;\u0001\u0000"+
		"\u0000\u0000\u0004=\u0001\u0000\u0000\u0000\u0006B\u0001\u0000\u0000\u0000"+
		"\bS\u0001\u0000\u0000\u0000\nU\u0001\u0000\u0000\u0000\f_\u0001\u0000"+
		"\u0000\u0000\u000ec\u0001\u0000\u0000\u0000\u0010e\u0001\u0000\u0000\u0000"+
		"\u0012\u0083\u0001\u0000\u0000\u0000\u0014\u0085\u0001\u0000\u0000\u0000"+
		"\u0016\u0089\u0001\u0000\u0000\u0000\u0018\u008b\u0001\u0000\u0000\u0000"+
		"\u001a\u008d\u0001\u0000\u0000\u0000\u001c\u0099\u0001\u0000\u0000\u0000"+
		"\u001e\u009e\u0001\u0000\u0000\u0000 \u00a2\u0001\u0000\u0000\u0000\""+
		"\u00a4\u0001\u0000\u0000\u0000$\u00a9\u0001\u0000\u0000\u0000&\u00ad\u0001"+
		"\u0000\u0000\u0000(\u00ba\u0001\u0000\u0000\u0000*\u00be\u0001\u0000\u0000"+
		"\u0000,-\u0005\r\u0000\u0000-.\u0005\u0001\u0000\u0000./\u0005\u000e\u0000"+
		"\u0000/1\u0003\u0002\u0001\u000002\u0003\u0004\u0002\u000010\u0001\u0000"+
		"\u0000\u000012\u0001\u0000\u0000\u000024\u0001\u0000\u0000\u000035\u0003"+
		"&\u0013\u000043\u0001\u0000\u0000\u000045\u0001\u0000\u0000\u000057\u0001"+
		"\u0000\u0000\u000068\u0003*\u0015\u000076\u0001\u0000\u0000\u000078\u0001"+
		"\u0000\u0000\u000089\u0001\u0000\u0000\u00009:\u0005\u0000\u0000\u0001"+
		":\u0001\u0001\u0000\u0000\u0000;<\u0007\u0000\u0000\u0000<\u0003\u0001"+
		"\u0000\u0000\u0000=>\u0005\u000f\u0000\u0000>@\u0003\u0006\u0003\u0000"+
		"?A\u0005\u0000\u0000\u0001@?\u0001\u0000\u0000\u0000@A\u0001\u0000\u0000"+
		"\u0000A\u0005\u0001\u0000\u0000\u0000BG\u0003\b\u0004\u0000CD\u0005\u0010"+
		"\u0000\u0000DF\u0003\b\u0004\u0000EC\u0001\u0000\u0000\u0000FI\u0001\u0000"+
		"\u0000\u0000GE\u0001\u0000\u0000\u0000GH\u0001\u0000\u0000\u0000H\u0007"+
		"\u0001\u0000\u0000\u0000IG\u0001\u0000\u0000\u0000JT\u0003\n\u0005\u0000"+
		"KT\u0003\u0010\b\u0000LT\u0003\u0012\t\u0000MT\u0003\u001a\r\u0000NT\u0003"+
		"$\u0012\u0000OP\u0005\u0002\u0000\u0000PQ\u0003\u0006\u0003\u0000QR\u0005"+
		"\u0003\u0000\u0000RT\u0001\u0000\u0000\u0000SJ\u0001\u0000\u0000\u0000"+
		"SK\u0001\u0000\u0000\u0000SL\u0001\u0000\u0000\u0000SM\u0001\u0000\u0000"+
		"\u0000SN\u0001\u0000\u0000\u0000SO\u0001\u0000\u0000\u0000T\t\u0001\u0000"+
		"\u0000\u0000UV\u0005\u0012\u0000\u0000VW\u0005\u0002\u0000\u0000WX\u0003"+
		"\f\u0006\u0000XY\u0005\u0004\u0000\u0000YZ\u0003\u000e\u0007\u0000Z[\u0005"+
		"\u0003\u0000\u0000[\u000b\u0001\u0000\u0000\u0000\\`\u0005\u001c\u0000"+
		"\u0000]`\u0003\u0002\u0001\u0000^`\u0005\u001b\u0000\u0000_\\\u0001\u0000"+
		"\u0000\u0000_]\u0001\u0000\u0000\u0000_^\u0001\u0000\u0000\u0000`\r\u0001"+
		"\u0000\u0000\u0000ad\u0005\u001c\u0000\u0000bd\u0003\"\u0011\u0000ca\u0001"+
		"\u0000\u0000\u0000cb\u0001\u0000\u0000\u0000d\u000f\u0001\u0000\u0000"+
		"\u0000ef\u0005\u0011\u0000\u0000fg\u0005\u0002\u0000\u0000gh\u0005\u001c"+
		"\u0000\u0000hi\u0005\u0003\u0000\u0000i\u0011\u0001\u0000\u0000\u0000"+
		"jk\u0005\u0013\u0000\u0000kl\u0005\u0002\u0000\u0000lm\u0003\"\u0011\u0000"+
		"mn\u0005\u0003\u0000\u0000no\u0003\u0014\n\u0000op\u0003\u0016\u000b\u0000"+
		"p\u0084\u0001\u0000\u0000\u0000qr\u0005\u0013\u0000\u0000rs\u0005\u0002"+
		"\u0000\u0000st\u0003\"\u0011\u0000tu\u0005\u0003\u0000\u0000u\u0084\u0001"+
		"\u0000\u0000\u0000vw\u0005\u0013\u0000\u0000w\u0084\u0005\u001c\u0000"+
		"\u0000xy\u0005\u0013\u0000\u0000yz\u0005\u0002\u0000\u0000z{\u0003\"\u0011"+
		"\u0000{|\u0005\u0003\u0000\u0000|}\u0005\u001a\u0000\u0000}~\u0005\u0002"+
		"\u0000\u0000~\u007f\u0003\u0016\u000b\u0000\u007f\u0080\u0005\u0004\u0000"+
		"\u0000\u0080\u0081\u0003\u0018\f\u0000\u0081\u0082\u0005\u0003\u0000\u0000"+
		"\u0082\u0084\u0001\u0000\u0000\u0000\u0083j\u0001\u0000\u0000\u0000\u0083"+
		"q\u0001\u0000\u0000\u0000\u0083v\u0001\u0000\u0000\u0000\u0083x\u0001"+
		"\u0000\u0000\u0000\u0084\u0013\u0001\u0000\u0000\u0000\u0085\u0086\u0007"+
		"\u0001\u0000\u0000\u0086\u0015\u0001\u0000\u0000\u0000\u0087\u008a\u0005"+
		"\u001c\u0000\u0000\u0088\u008a\u0003$\u0012\u0000\u0089\u0087\u0001\u0000"+
		"\u0000\u0000\u0089\u0088\u0001\u0000\u0000\u0000\u008a\u0017\u0001\u0000"+
		"\u0000\u0000\u008b\u008c\u0005\u001c\u0000\u0000\u008c\u0019\u0001\u0000"+
		"\u0000\u0000\u008d\u008e\u0005\u0014\u0000\u0000\u008e\u008f\u0005\u0002"+
		"\u0000\u0000\u008f\u0090\u0003\u001c\u000e\u0000\u0090\u0091\u0005\u0004"+
		"\u0000\u0000\u0091\u0092\u0003 \u0010\u0000\u0092\u0093\u0005\u0004\u0000"+
		"\u0000\u0093\u0094\u0003\u001e\u000f\u0000\u0094\u0095\u0005\u0003\u0000"+
		"\u0000\u0095\u001b\u0001\u0000\u0000\u0000\u0096\u009a\u0003\"\u0011\u0000"+
		"\u0097\u009a\u0005\u001c\u0000\u0000\u0098\u009a\u0003\u0002\u0001\u0000"+
		"\u0099\u0096\u0001\u0000\u0000\u0000\u0099\u0097\u0001\u0000\u0000\u0000"+
		"\u0099\u0098\u0001\u0000\u0000\u0000\u009a\u001d\u0001\u0000\u0000\u0000"+
		"\u009b\u009f\u0003\"\u0011\u0000\u009c\u009f\u0005\u001c\u0000\u0000\u009d"+
		"\u009f\u0003\u0002\u0001\u0000\u009e\u009b\u0001\u0000\u0000\u0000\u009e"+
		"\u009c\u0001\u0000\u0000\u0000\u009e\u009d\u0001\u0000\u0000\u0000\u009f"+
		"\u001f\u0001\u0000\u0000\u0000\u00a0\u00a3\u0005\u001c\u0000\u0000\u00a1"+
		"\u00a3\u0003\u0002\u0001\u0000\u00a2\u00a0\u0001\u0000\u0000\u0000\u00a2"+
		"\u00a1\u0001\u0000\u0000\u0000\u00a3!\u0001\u0000\u0000\u0000\u00a4\u00a5"+
		"\u0005\n\u0000\u0000\u00a5\u00a7\u0005\u001d\u0000\u0000\u00a6\u00a8\u0005"+
		"\u001b\u0000\u0000\u00a7\u00a6\u0001\u0000\u0000\u0000\u00a7\u00a8\u0001"+
		"\u0000\u0000\u0000\u00a8#\u0001\u0000\u0000\u0000\u00a9\u00aa\u0005\u000b"+
		"\u0000\u0000\u00aa\u00ab\u0003\u0000\u0000\u0000\u00ab\u00ac\u0005\f\u0000"+
		"\u0000\u00ac%\u0001\u0000\u0000\u0000\u00ad\u00ae\u0005\u0015\u0000\u0000"+
		"\u00ae\u00af\u0005\u0016\u0000\u0000\u00af\u00b4\u0003(\u0014\u0000\u00b0"+
		"\u00b1\u0005\u0004\u0000\u0000\u00b1\u00b3\u0003(\u0014\u0000\u00b2\u00b0"+
		"\u0001\u0000\u0000\u0000\u00b3\u00b6\u0001\u0000\u0000\u0000\u00b4\u00b2"+
		"\u0001\u0000\u0000\u0000\u00b4\u00b5\u0001\u0000\u0000\u0000\u00b5\u00b8"+
		"\u0001\u0000\u0000\u0000\u00b6\u00b4\u0001\u0000\u0000\u0000\u00b7\u00b9"+
		"\u0005\u0000\u0000\u0001\u00b8\u00b7\u0001\u0000\u0000\u0000\u00b8\u00b9"+
		"\u0001\u0000\u0000\u0000\u00b9\'\u0001\u0000\u0000\u0000\u00ba\u00bc\u0003"+
		"\u0002\u0001\u0000\u00bb\u00bd\u0007\u0002\u0000\u0000\u00bc\u00bb\u0001"+
		"\u0000\u0000\u0000\u00bc\u00bd\u0001\u0000\u0000\u0000\u00bd)\u0001\u0000"+
		"\u0000\u0000\u00be\u00bf\u0005\u0019\u0000\u0000\u00bf\u00c1\u0005\u001e"+
		"\u0000\u0000\u00c0\u00c2\u0005\u0000\u0000\u0001\u00c1\u00c0\u0001\u0000"+
		"\u0000\u0000\u00c1\u00c2\u0001\u0000\u0000\u0000\u00c2+\u0001\u0000\u0000"+
		"\u0000\u0012147@GS_c\u0083\u0089\u0099\u009e\u00a2\u00a7\u00b4\u00b8\u00bc"+
		"\u00c1";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}