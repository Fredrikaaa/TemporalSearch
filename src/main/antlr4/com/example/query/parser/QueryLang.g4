/**
 * Grammar for a SQL-like query language specialized for NLP operations.
 * Supports operations like named entity recognition (NER), text search,
 * temporal queries, and dependency parsing.
 * 
 * The grammar follows these main patterns from ANTLR documentation:
 * - Sequence: Basic SQL structure (SELECT FROM WHERE)
 * - Choice: Different types of conditions
 * - Token dependency: Matching parentheses and quotes
 * - Nested phrase: Subqueries and nested conditions
 */
grammar QueryLang;

// Parser Rules

/**
 * The root rule for a query. Follows SQL-like structure with SELECT, FROM, WHERE
 * and optional ORDER BY and LIMIT clauses.
 * Example: SELECT PERSON, DATE, SNIPPET() FROM corpus WHERE CONTAINS("AI")
 * Pattern: Sequence with optional elements
 */
query
    : SELECT columns=columnList FROM source=identifier
      whereClause?
      orderByClause?
      limitClause?
      EOF
    ;

/**
 * Column specification list
 * Example: PERSON AS name, DATE, SNIPPET(LENGTH=100)
 * Pattern: Sequence with separator (comma)
 */
columnList
    : first=columnSpec (',' next+=columnSpec)*
    | '*'  // Select all available columns
    ;

/**
 * Single column specification with optional alias
 * Example: PERSON AS politician
 * Pattern: Column type with optional alias and options
 */
columnSpec
    : columnType (AS alias=identifier)?  // Basic column with optional alias
    | SNIPPET '(' snippetOptions? ')'    // Snippet with options
    | COUNT '(' countTarget? ')'         // Count aggregation
    ;

/**
 * Available column types for value extraction
 */
columnType
    : PERSON    // Person name extraction
    | DATE      // Date value extraction  
    | LOCATION  // Location name extraction
    | TERM      // Matched term
    | RELATION  // Dependency relation
    | CATEGORY  // Hypernym category
    ;

/**
 * Snippet configuration options
 * Example: SNIPPET(LENGTH=100, HIGHLIGHT="*")
 */
snippetOptions
    : first=snippetOption (',' next+=snippetOption)*
    ;

/**
 * Individual snippet option
 */
snippetOption
    : LENGTH '=' NUMBER     // Context length
    | CONTEXT '=' NUMBER    // Words of context
    | HIGHLIGHT '=' STRING  // Highlight style
    | FORMAT '=' STRING     // Output format
    ;

/**
 * Count aggregation target
 */
countTarget
    : MATCHES              // Count total matches
    | UNIQUE columnType    // Count unique values
    | DOCUMENTS           // Count matching documents
    ;

/**
 * Matches either a plain identifier or a quoted string.
 * Examples: myCorpus, "my corpus"
 * Pattern: Choice between token types
 */
identifier
    : IDENTIFIER
    | STRING
    ;

/**
 * The WHERE clause containing a list of conditions.
 * Example: WHERE NER("PERSON", "?person") AND CONTAINS("scientist")
 * Pattern: Sequence with mandatory WHERE keyword and conditions
 */
whereClause
    : WHERE conditions=conditionList EOF?
    ;

/**
 * A list of conditions joined by AND operators.
 * Example: condition1 AND condition2 AND condition3
 * Pattern: Sequence with separator (AND)
 */
conditionList
    : first=singleCondition (AND next+=singleCondition)*
    ;

/**
 * A single condition without AND.
 * Pattern: Choice between different condition types
 */
singleCondition
    : nerExpression
    | containsExpression  
    | dateExpression
    | dependsExpression
    | subQuery
    | '(' nested=conditionList ')'  // Nested conditions
    ;

/**
 * Named Entity Recognition (NER) expression.
 * Matches entities of a specific type.
 * Example: NER("PERSON", "?person") or NER("ORGANIZATION", "Google")
 * Pattern: Token dependency (matching parentheses)
 */
nerExpression
    : NER '(' type=entityType ',' target=entityTarget ')'
    ;

/**
 * The type of entity to match in NER expressions.
 * Can be a string, identifier, or wildcard (*).
 * Examples: "PERSON", ORGANIZATION, *
 * Pattern: Choice between token types
 */
entityType
    : STRING
    | identifier
    | WILDCARD
    ;

/**
 * The target entity to match or variable to bind.
 * Examples: "Google", ?company
 * Pattern: Choice between string and variable
 */
entityTarget 
    : STRING
    | variable
    ;

/**
 * Text search expression.
 * Example: CONTAINS("artificial intelligence")
 * Pattern: Token dependency (matching parentheses)
 */
containsExpression
    : CONTAINS '(' text=STRING ')'
    ;

/**
 * Date/temporal expressions with various forms:
 * 1. Comparison: DATE("?date") < "2020"
 * 2. Variable binding: DATE("?date")
 * 3. Direct date: DATE "2020"
 * 4. Near with range: DATE("?date") NEAR("1980", "5y")
 * Pattern: Choice between different date expressions
 */
dateExpression
    : DATE '(' dateVar=variable ')' dateOp=dateOperator dateCompareValue=dateValue  // Comparison
    | DATE '(' dateVar=variable ')'                                // Variable binding
    | DATE dateString=STRING                                          // Direct date
    | DATE '(' dateVar=variable ')' NEAR '(' dateCompareValue=dateValue ',' dateRange=rangeSpec ')'  // NEAR with range
    ;

/**
 * Comparison operators for dates.
 * Pattern: Choice between operators
 */
dateOperator
    : '<' | '>' | '<=' | '>=' | '=='
    ;

/**
 * The value to compare dates against.
 * Can be a string date or a subquery.
 * Pattern: Choice between string and subquery
 */
dateValue
    : STRING
    | subQuery
    ;

/**
 * Range specification for NEAR operations.
 * Example: "5y" for 5 years
 * Pattern: Simple string token
 */
rangeSpec
    : STRING
    ;

/**
 * Dependency parsing expression.
 * Matches relationships between words/phrases.
 * Example: DEPENDS("?org", "founded", "?date")
 * Pattern: Token dependency (matching parentheses)
 */
dependsExpression
    : DEPENDS '(' gov=governor ',' rel=relation ',' dep=dependent ')'
    ;

/**
 * The governor (head) in a dependency relation.
 * Can be a variable or identifier.
 * Pattern: Choice between variable, string, and identifier
 */
governor
    : variable
    | STRING
    | identifier
    ;

/**
 * The dependent (child) in a dependency relation.
 * Can be a variable or identifier.
 * Pattern: Choice between variable, string, and identifier
 */
dependent
    : variable
    | STRING
    | identifier
    ;

/**
 * The type of relationship in a dependency.
 * Example: "founded", "owns", etc.
 * Pattern: Choice between string and identifier
 */
relation
    : STRING
    | identifier
    ;

/**
 * Variable definition with optional wildcard.
 * Examples: ?person, ?org*
 * Pattern: Sequence with optional wildcard
 */
variable
    : '?' name=IDENTIFIER wild=WILDCARD?  // Matches ?person*
    ;

/**
 * Nested subquery in curly braces.
 * Example: { SELECT documents FROM corpus WHERE ... }
 * Pattern: Nested phrase (recursive query)
 */
subQuery
    : '{' nested=query '}'
    ;

/**
 * ORDER BY clause for result ordering.
 * Example: ORDER BY date DESC
 * Pattern: Sequence with separator (comma)
 */
orderByClause
    : ORDER BY first=orderSpec (',' next+=orderSpec)* EOF?
    ;

/**
 * Specification for ordering results.
 * Example: date DESC
 * Pattern: Sequence with optional direction
 */
orderSpec
    : field=identifier dir=(ASC | DESC)?
    ;

/**
 * LIMIT clause to restrict number of results.
 * Example: LIMIT 10
 * Pattern: Simple sequence
 */
limitClause
    : LIMIT count=NUMBER EOF?
    ;

// Lexer Rules

// Keywords
SELECT: 'SELECT';
FROM: 'FROM';
WHERE: 'WHERE';
AND: 'AND';
CONTAINS: 'CONTAINS';
NER: 'NER';
DATE: 'DATE';
DEPENDS: 'DEPENDS';
ORDER: 'ORDER';
BY: 'BY';
ASC: 'ASC';
DESC: 'DESC';
LIMIT: 'LIMIT';
NEAR: 'NEAR';

// Special characters
WILDCARD: '*';  // Wildcard character

// Basic tokens
STRING: '"' (~["\\] | '\\' .)* '"';  // Quoted strings with escape support
IDENTIFIER: [a-zA-Z_][a-zA-Z0-9_]*;  // Standard identifier pattern
NUMBER: [0-9]+;  // Integer numbers
WS: [ \t\r\n]+ -> skip;  // Ignore whitespace
COMMENT: '//' ~[\r\n]* -> skip;  // Single-line comments

// Add new lexer rules at the end
PERSON: 'PERSON';
LOCATION: 'LOCATION';
TERM: 'TERM';
RELATION: 'RELATION';
CATEGORY: 'CATEGORY';
SNIPPET: 'SNIPPET';
COUNT: 'COUNT';
LENGTH: 'LENGTH';
CONTEXT: 'CONTEXT';
HIGHLIGHT: 'HIGHLIGHT';
FORMAT: 'FORMAT';
MATCHES: 'MATCHES';
UNIQUE: 'UNIQUE';
DOCUMENTS: 'DOCUMENTS';
AS: 'AS'; 