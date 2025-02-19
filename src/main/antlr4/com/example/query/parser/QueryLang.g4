grammar QueryLang;

// Parser Rules
query
    : SELECT 'documents' FROM source=identifier
      whereClause?
      orderByClause?
      limitClause?
    ;

identifier
    : IDENTIFIER
    | STRING
    ;

whereClause
    : WHERE condition+
    ;

condition
    : containsExpression
    | nerExpression
    | temporalExpression
    | dependencyExpression
    | subQuery
    | '(' condition+ ')'
    ;

containsExpression
    : CONTAINS '(' value=STRING ')'
    ;

nerExpression
    : NER '(' type=nerType ',' target=nerTarget ')'
    ;

nerType
    : identifier
    | WILDCARD
    ;

nerTarget
    : identifier
    | variable
    ;

temporalExpression
    : TEMPORAL '(' dateSpec=temporalSpec ')'
    | DATE '(' dateVar=variable ')'
    | DATE '(' dateVar=variable ')' temporalOperator dateValue
    ;

temporalSpec
    : BEFORE date=temporalValue
    | AFTER date=temporalValue
    | BETWEEN start=temporalValue AND end=temporalValue
    | NEAR date=temporalValue ',' range=STRING
    ;

temporalValue
    : STRING
    | variable
    ;

temporalOperator
    : BEFORE
    | AFTER
    | BETWEEN
    | NEAR
    ;

dateValue
    : STRING
    | subQuery
    ;

dependencyExpression
    : DEPENDENCY '(' governor=depComponent ',' relation=identifier ',' dependent=depComponent ')'
    ;

depComponent
    : identifier
    | variable
    ;

variable
    : VARIABLE name=IDENTIFIER variableModifier?
    ;

variableModifier
    : WILDCARD    // For pattern matching
    | OPTIONAL    // For optional matches
    ;

subQuery
    : '{' query '}'
    ;

orderByClause
    : ORDER BY orderSpec (',' orderSpec)*
    ;

orderSpec
    : field=identifier (ASC | DESC)?
    ;

limitClause
    : LIMIT count=NUMBER
    ;

// Lexer Rules
SELECT: 'SELECT';
FROM: 'FROM';
WHERE: 'WHERE';
CONTAINS: 'CONTAINS';
NER: 'NER';
TEMPORAL: 'TEMPORAL';
DATE: 'DATE';
DEPENDENCY: 'DEPENDENCY';
ORDER: 'ORDER';
BY: 'BY';
ASC: 'ASC';
DESC: 'DESC';
LIMIT: 'LIMIT';
BEFORE: 'BEFORE';
AFTER: 'AFTER';
BETWEEN: 'BETWEEN';
NEAR: 'NEAR';
AND: 'AND';

VARIABLE: '?';
WILDCARD: '*';
OPTIONAL: '?';

STRING: '"' (~["\\] | '\\' .)* '"';
IDENTIFIER: [a-zA-Z_][a-zA-Z0-9_]*;
NUMBER: [0-9]+;
WS: [ \t\r\n]+ -> skip;
COMMENT: '//' ~[\r\n]* -> skip; 