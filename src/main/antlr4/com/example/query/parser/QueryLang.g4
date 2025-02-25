grammar QueryLang;

// Lexer Rules (Tokens)
// Keywords
SELECT: 'SELECT';
FROM: 'FROM';
WHERE: 'WHERE';
SNIPPET: 'SNIPPET';
WINDOW: 'WINDOW';
NER: 'NER';
DEPENDS: 'DEPENDS';
DATE: 'DATE';
NEAR: 'NEAR';
BETWEEN: 'BETWEEN';
GRANULARITY: 'GRANULARITY';
DOCUMENT: 'DOCUMENT';
SENTENCE: 'SENTENCE';
METADATA: 'METADATA';
CONTAINS: 'CONTAINS';
CONTAINED_BY: 'CONTAINED_BY';
INTERSECT: 'INTERSECT';
RADIUS: 'RADIUS';
AND: 'AND';
ORDER: 'ORDER';
BY: 'BY';
ASC: 'ASC';
DESC: 'DESC';
LIMIT: 'LIMIT';
AS: 'AS';
COUNT: 'COUNT';
UNIQUE: 'UNIQUE';
DOCUMENTS: 'DOCUMENTS';

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
      whereClause?
      granularityClause?
      EOF
    ;

selectList
    : selectColumn (',' selectColumn)*
    ;

selectColumn
    : variable
    | snippetExpression
    | metadataExpression
    | identifier
    ;

snippetExpression
    : SNIPPET LPAREN variable (COMMA WINDOW EQUALS NUMBER)? RPAREN
    ;

metadataExpression
    : METADATA
    ;

whereClause
    : WHERE conditionList
    ;

conditionList
    : singleCondition (AND singleCondition)*
    ;

singleCondition
    : nerExpression
    | containsExpression
    | dateExpression
    | dependsExpression
    | LPAREN conditionList RPAREN
    ;

dateExpression
    : DATE LPAREN variable COMMA operator=dateOperator range=dateValue
      (RADIUS radius=NUMBER unit=timeUnit)? RPAREN
    | DATE LPAREN variable COMMA comparisonOp year=NUMBER RPAREN
    ;

dateOperator
    : CONTAINS
    | CONTAINED_BY
    | INTERSECT
    | NEAR
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

nerExpression
    : NER LPAREN type=entityType COMMA target=entityTarget RPAREN
    ;

entityType
    : STRING
    | identifier
    | WILDCARD
    ;

entityTarget 
    : STRING
    | variable
    ;

containsExpression
    : CONTAINS LPAREN terms+=STRING (COMMA terms+=STRING)* RPAREN
    ;

dependsExpression
    : DEPENDS LPAREN gov=governor COMMA rel=relation COMMA dep=dependent RPAREN
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
    | STRING
    ;

comparisonOp
    : LT | GT | LE | GE | EQ
    ; 