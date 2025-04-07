grammar QueryLang;

/*
 * This grammar supports variable binding using the AS keyword pattern.
 * Example: NER(PERSON) AS ?person
 * The AS keyword followed by a variable name binds the result of a condition to a variable.
 * All variables must be prefixed with ? character.
 * Variables can be used in SELECT clause and subsequent WHERE conditions.
 * 
 * Variable binding flow:
 * 1. Variables are produced by conditions using the AS ?var syntax
 * 2. Variables can be consumed by other conditions that reference them
 * 3. Variables can be used in the SELECT clause to display results
 * 4. Type safety is enforced through the VariableRegistry
 * 
 * This grammar also supports subqueries and joins:
 * - Subqueries are defined using parentheses around a full query
 * - Joins are specified using the JOIN keyword
 * - Temporal join conditions use predicates like CONTAINS, CONTAINED_BY, INTERSECT
 */

// Lexer Rules (Tokens)
// Keywords
SELECT: 'SELECT';
FROM: 'FROM';
WHERE: 'WHERE';
SNIPPET: 'SNIPPET';
WINDOW: 'WINDOW';
NER: 'NER';
POS: 'POS';
DEPENDS: 'DEPENDS';
DATE: 'DATE';
PROXIMITY: 'PROXIMITY';
GRANULARITY: 'GRANULARITY';
DOCUMENT: 'DOCUMENT';
SENTENCE: 'SENTENCE';
TITLE: 'TITLE';
TIMESTAMP: 'TIMESTAMP';
CONTAINS: 'CONTAINS';
CONTAINED_BY: 'CONTAINED_BY';
INTERSECT: 'INTERSECT';
RADIUS: 'RADIUS';
AND: 'AND';
OR: 'OR';
NOT: 'NOT';
ORDER: 'ORDER';
BY: 'BY';
ASC: 'ASC';
DESC: 'DESC';
LIMIT: 'LIMIT';
AS: 'AS';
COUNT: 'COUNT';
UNIQUE: 'UNIQUE';
DOCUMENTS: 'DOCUMENTS';
JOIN: 'JOIN';
ON: 'ON';
INNER: 'INNER';
LEFT: 'LEFT';
RIGHT: 'RIGHT';

// NER types
PERSON: 'PERSON';
LOCATION: 'LOCATION';
ORGANIZATION: 'ORGANIZATION';
TIME: 'TIME';
MONEY: 'MONEY';
PERCENT: 'PERCENT';

// Structure tokens
LPAREN: '(';
RPAREN: ')';
COMMA: ',';
EQUALS: '=';
LBRACKET: '[';
RBRACKET: ']';
WILDCARD: '*';
QUESTION: '?';
LT: '<';
GT: '>';
LE: '<=';
GE: '>=';
EQ: '==';

// Time units
YEAR: 'y';
DAY: 'd';

// Basic tokens
IDENTIFIER: [a-zA-Z_][a-zA-Z0-9_]*;
STRING: '"' (~["\r\n])* '"';
NUMBER: [0-9]+;

// Skip whitespace and comments
WS: [ \t\r\n]+ -> skip;
COMMENT: '//' ~[\r\n]* -> skip;
BLOCK_COMMENT: '/*' .*? '*/' -> skip;

// Parser Rules

query
    : SELECT selectList
      FROM identifier
      joinClause*
      whereClause?
      granularityClause?
      orderByClause?
      limitClause?
      EOF
    ;

selectList
    : selectColumn (',' selectColumn)*
    ;

selectColumn
    : variable                                  # VariableColumn
    | snippetExpression                         # SnippetColumn
    | titleExpression                           # TitleColumn
    | timestampExpression                       # TimestampColumn
    | countExpression                           # CountColumn
    ;

snippetExpression
    : SNIPPET LPAREN variable (COMMA WINDOW EQUALS NUMBER)? RPAREN
    ;

titleExpression
    : TITLE
    ;

timestampExpression
    : TIMESTAMP
    ;

countExpression
    : COUNT LPAREN WILDCARD RPAREN                   # CountAllExpression
    | COUNT LPAREN UNIQUE variable RPAREN            # CountUniqueExpression
    | COUNT LPAREN DOCUMENTS RPAREN                  # CountDocumentsExpression
    ;

whereClause
    : WHERE conditionList
    ;

conditionList
    : condition (logicalOp condition)*
    ;

condition
    : notCondition
    | atomicCondition
    ;

notCondition
    : NOT atomicCondition
    ;

atomicCondition
    : singleCondition
    | LPAREN conditionList RPAREN
    ;

logicalOp
    : AND
    | OR
    ;

singleCondition
    : nerExpression
    | containsExpression
    | dateExpression
    | dependsExpression
    | posExpression
    ;

dateExpression
    : DATE LPAREN comparisonOp year=NUMBER RPAREN (AS var=variable)?        # DateComparisonExpression
    | DATE LPAREN dateOperator dateValue
      (RADIUS radius=NUMBER unit=timeUnit)? RPAREN (AS var=variable)?       # DateOperatorExpression
    ;

dateOperator
    : CONTAINS
    | CONTAINED_BY
    | INTERSECT
    | PROXIMITY
    ;

dateValue
    : LBRACKET start=NUMBER COMMA end=NUMBER RBRACKET  # DateRange
    | single=NUMBER                                     # SingleYear
    ;

timeUnit
    : YEAR
    | DAY
    ;

granularityClause
    : GRANULARITY (DOCUMENT | SENTENCE NUMBER?)
    ;

orderByClause
    : ORDER BY orderSpec (COMMA orderSpec)*
    ;

// Order specification rule
// The visitor implementation converts this to strings with "-" prefix for DESC order
// Example: "column_name" for ASC, "-column_name" for DESC
orderSpec
    : (qualifiedIdentifier | identifier | variable) (ASC | DESC)?
    ;

qualifiedIdentifier
    : identifier '.' (identifier | variable)
    ;

limitClause
    : LIMIT count=NUMBER
    ;

nerExpression
    : NER LPAREN type=entityType (COMMA termValue=term)? RPAREN (AS var=variable)?
    ;

entityType
    : PERSON
    | LOCATION
    | ORGANIZATION
    | TIME
    | MONEY
    | PERCENT
    | WILDCARD
    | STRING
    | IDENTIFIER
    ;

containsExpression
    : CONTAINS LPAREN terms+=STRING (COMMA terms+=STRING)* RPAREN (AS var=variable)?
    ;

dependsExpression
    : DEPENDS LPAREN gov=governor COMMA rel=relation COMMA dep=dependent RPAREN (AS var=variable)?
    ;

governor
    : variable
    | STRING
    | identifier
    ;

relation
    : STRING
    | identifier
    ;

dependent
    : variable
    | STRING
    | identifier
    ;

variable
    : QUESTION IDENTIFIER
    ;

identifier
    : IDENTIFIER
    ;

comparisonOp
    : LT
    | GT
    | LE
    | GE
    | EQ
    | EQUALS
    ;

term
    : STRING
    | variable
    | identifier
    ;

// Subquery and join syntax
joinClause
    : joinType? JOIN subquery ON joinCondition
    ;

joinType
    : INNER
    | LEFT
    | RIGHT
    ;

subquery
    : LPAREN
      SELECT selectList
      FROM identifier
      whereClause?
      RPAREN
      AS alias=identifier
    ;

joinCondition
    : leftColumn=joinColumn temporalOp rightColumn=joinColumn
      (WINDOW window=NUMBER)?
    ;

joinColumn
    : qualifiedIdentifier
    | variable
    ;

temporalOp
    : CONTAINS
    | CONTAINED_BY
    | INTERSECT
    | PROXIMITY
    ;

posExpression
    : POS LPAREN tag=posTag (COMMA termValue=term)? RPAREN (AS var=variable)?
    ;

posTag
    : STRING
    | identifier
    ; 