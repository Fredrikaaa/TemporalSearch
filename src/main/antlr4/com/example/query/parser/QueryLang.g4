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
    | '(' condition+ ')'
    ;

containsExpression
    : CONTAINS '(' value=STRING ')'
    ;

nerExpression
    : NER '(' type=identifier ',' identTarget=identifier ')'
    | NER '(' type=identifier ',' varTarget=variable ')'
    ;

temporalExpression
    : TEMPORAL '(' dateSpec=temporalSpec ')'
    ;

temporalSpec
    : BEFORE date=STRING
    | AFTER date=STRING
    | BETWEEN start=STRING AND end=STRING
    ;

dependencyExpression
    : DEPENDENCY '(' governor=identifier ',' relation=identifier ',' dependent=identifier ')'
    ;

variable
    : '?' name=IDENTIFIER ('*' | '?')?
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
DEPENDENCY: 'DEPENDENCY';
ORDER: 'ORDER';
BY: 'BY';
ASC: 'ASC';
DESC: 'DESC';
LIMIT: 'LIMIT';
BEFORE: 'BEFORE';
AFTER: 'AFTER';
BETWEEN: 'BETWEEN';
AND: 'AND';

STRING: '"' (~["\\] | '\\' .)* '"';
IDENTIFIER: [a-zA-Z_][a-zA-Z0-9_]*;
NUMBER: [0-9]+;
WS: [ \t\r\n]+ -> skip;
COMMENT: '//' ~[\r\n]* -> skip; 