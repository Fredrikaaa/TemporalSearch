grammar QueryLang;

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
    : variable                    # VariableColumn
    | snippetExpression           # SnippetColumn
    | metadataExpression          # MetadataColumn
    | countExpression             # CountColumn
    | identifier                  # IdentifierColumn
    ;

snippetExpression
    : SNIPPET LPAREN variable (COMMA WINDOW EQUALS NUMBER)? RPAREN
    ;

metadataExpression
    : METADATA
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
    : DATE LPAREN variable COMMA comparisonOp year=NUMBER RPAREN       # DateComparisonExpression
    | DATE LPAREN variable COMMA dateOperator dateValue
      (RADIUS radius=NUMBER unit=timeUnit)? RPAREN                     # DateOperatorExpression
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

orderByClause
    : ORDER BY orderSpec (COMMA orderSpec)*
    ;

orderSpec
    : (identifier | variable) (ASC | DESC)?
    ;

limitClause
    : LIMIT count=NUMBER
    ;

nerExpression
    : NER LPAREN type=entityType (COMMA target=entityTarget)? RPAREN
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

entityTarget 
    : STRING
    | variable
    ;

containsExpression
    : CONTAINS LPAREN variable COMMA terms+=STRING RPAREN                        # ContainsWithVariableExpression
    | CONTAINS LPAREN terms+=STRING (COMMA terms+=STRING)* RPAREN                # ContainsWithoutVariableExpression
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

posExpression
    : POS LPAREN tag=posTag COMMA termValue=term RPAREN
    ;

posTag
    : STRING
    | identifier
    ;

term
    : STRING
    | variable
    | identifier
    ; 