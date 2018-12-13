grammar tsl; // TSL grammar
import tslTokens; // includes all rules from TslTokens.g4

// Start to parse a TSL script
prog:   stat* ;

// Split per TSL query and store variable
stat:  expr
    |  (WS|COMMENT)        
    ;

// A valid TSL expr is composed of 
// - a simple select statement
// - a connect statement
// - a more complex statement starting by an operation 
// - a more complex statement starting by a mask 

expr:  selectExpr
    |  connectExpr (DOT selectExpr)?
    |  opExpr
    |  form
    ;

form: IDENT '=' expr
    |   IDENT '=' basic
    | IDENT seriesOperations*
    ;

// A basic type supported on a TSL statement
basic: STRING
    | PROM | PROMETHEUS | WARP
    | NUMBER
    | TRUE | FALSE
    | EMPTY_LIST
    | STRING_LIST
    | ASTERISK
    | DURATIONVAL
    ;

// The connect statement expression
// - with a basic auth (user:password)
// - with a token
connectExpr:  CONNECT LPAREN type=(PROM|PROMETHEUS|WARP|IDENT) COMMA  api=(STRING|IDENT) COMMA user=(STRING|IDENT) COMMA password=(STRING|IDENT) RPAREN 
    | CONNECT LPAREN type=(PROM|PROMETHEUS|WARP|IDENT) COMMA  api=(STRING|IDENT) COMMA token=(STRING|IDENT) RPAREN
    ;

// A complet select statement expression can be followed by series operations
selectExpr : selectSingleExpr seriesOperations*
    ;

// Manage minimal SELECT expression and it's direct child post clauses
selectSingleExpr : SELECT LPAREN (ASTERISK|STRING|IDENT) RPAREN postSelect*
    ;

// Handle where clauses and Time clauses for current select statement
postSelect : whereExpr
    | whereExpr* lastExpr whereExpr*
    | whereExpr* fromExpr whereExpr*
    ;

// Last time clause support 
// - last N minutes for a select statement
// - last N points for a select statement
// optionals parameters as shift (to shift all series ticks), timestamp or date to change NOW by specified time
lastExpr : DOT LAST LPAREN (DURATIONVAL|NUMBER|IDENT) RPAREN
    | DOT LAST LPAREN (DURATIONVAL|NUMBER|IDENT) COMMA ('shift' '=')? shift=(DURATIONVAL|IDENT) RPAREN
    | DOT LAST LPAREN (DURATIONVAL|NUMBER|IDENT) COMMA ('timestamp' '=')? timestamp=(NUMBER|IDENT) RPAREN
    | DOT LAST LPAREN (DURATIONVAL|NUMBER|IDENT) COMMA ('date' '=')? date=(STRING|IDENT) RPAREN
    ;

// From time clause support 
// - from N to X minutes for a select statement
// - from N to NOW for a select statement
fromExpr: DOT FROM LPAREN (NUMBER|STRING|IDENT) (COMMA ('to' '=')? to=(NUMBER|STRING|IDENT))? RPAREN
    ; 

// Select where clauses to select specific metrics based on their tags
whereExpr : DOT WHERE LPAREN (STRING|STRING_LIST|EMPTY_LIST|IDENT) RPAREN
    ;

// Times series fetched by select statement specific operations
// - as sampling
// - as group 
// - as groupBy or groupWithout
// - as window or cumulative
// - as arithmetic operation
// - as simple operation
// - as complex operation
seriesOperations : samplingOperations
    | groupOperation
    | groupByOperation
    | windowOperation
    | arithmeticOperationWithParam
    | simpleOperation
    | stringOperation
    | complexOperation
    ;

// Full sampling operation with optionals params
samplingOperations: prefixSampling sampleParam? sampleParam? sampleParam? RPAREN
    ;

// Optionals sampling params 
sampleParam: fillSampling | COMMA spanSampling | COMMA countSampling | relativeSampling
    ;

// Sampling fill param
fillSampling:  COMMA ('fill' '=')? fill=(STRING|STRING_LIST|EMPTY_LIST|IDENT)
    ;

// Sampling relative param
relativeSampling: COMMA ('relative' '=')? relative=(TRUE | FALSE | IDENT) 
    ;

// Sampling count param
countSampling: ('count' '=')? count=NUMBER
    ;

// Sampling span param
spanSampling: ('span' '=')? span=DURATIONVAL
    ;

// Minimal sampling operation
prefixSampling: DOT SAMPLEBY LPAREN (spanSampling|countSampling) COMMA ('aggregator' '=')? aggregator=aggregators
    ;

// Group operation
groupOperation: DOT GROUP LPAREN ('aggregator' '=')? aggregator=aggregators RPAREN
    ;

// GroupBy operation
groupByOperation: DOT (GROUPBY|GROUPWITHOUT) LPAREN tagkey=(STRING|EMPTY_LIST|STRING_LIST|IDENT) COMMA ('aggregator' '=')? aggregator=aggregators RPAREN
    ;

// Window/cumulative operation
windowOperation: DOT (WINDOW|CUMULATIVE) LPAREN ('aggregator' '=')? aggregator=windowAggregators COMMA ('pre' '=')? pre=(DURATIONVAL|NUMBER|IDENT) (COMMA ('post' '=')? post=(DURATIONVAL|NUMBER|IDENT))? RPAREN
    ;

// Window/cumulative specific aggregators
windowAggregators: DELTA | STDDEV | STDVAR
    | aggregators
    ;


// Aggregators available for operations like sampling, grouping
aggregators: MIN | MAX | MEAN | FIRST | LAST | SUM | (STRING | IDENT) COMMA JOIN | MEDIAN | COUNT | ANDL | ORL | NUMBER COMMA PERCENTILE | STRING | IDENT
    ;

// Arithmetic operations with a number parameter
arithmeticOperationWithParam: DOT arithmeticOperatorWithParam LPAREN value=(NUMBER|IDENT) RPAREN
    ;

// Arithmetic operator with a single number parameter
arithmeticOperatorWithParam: (ADDSERIES | SUBSERIES | MULSERIES | DIVSERIES | LOGN | EQUAL | NOTEQUAL | GREATERTHAN | GREATEROREQUAL | LESSTHAN 
    | LESSOREQUAL | MAXWITH | MINWITH | TOPN | BOTTOMN | SHRINK | TIMESCALE)
    ;

// Simple operations
simpleOperation: DOT simpleOperator LPAREN RPAREN
    ;

// Operator that does not require a parameter
simpleOperator: ABS CEIL CUMULATIVESUM FLOOR RESETS ROUND LN LOG2 LOG10 SQRT DAY WEEKDAY HOUR MINUTE MONTH YEAR TIMESTAMP SORT SORTDESC 
    ;

// Operation with a single string parameter
stringOperation: DOT stringOperator LPAREN value=(STRING|IDENT) RPAREN
    ;

stringOperator: ADDNAMEPREFIX | ADDNAMESUFFIX | RENAME | RENAMEBY | STORE | FILTERBYNAME | FILTERBYLASTVALUE
    ;
// Complex operation fix the grammar rule for unique TSL functions as
// - rate
// - shift
// - and/or 
// - sortBy and sortDescBy
// - topNBy and bottomNBy
// - removeLabels / renameLabelKey / renameLabelValue
// - timeclip / timesplit / timemodulo
complexOperation: DOT RATE LPAREN (DURATIONVAL|IDENT)? RPAREN
    | DOT SHIFT LPAREN (DURATIONVAL|IDENT) RPAREN
    | DOT (ANDL|ORL) LPAREN (TRUE|FALSE) RPAREN
    | DOT (SORTBY|SORTDESCBY) LPAREN aggregators RPAREN
    | DOT (TOPNBY|BOTTOMNBY) LPAREN (NUMBER|IDENT) COMMA aggregators RPAREN
    | DOT (REMOVELABELS|FILTERBYLABELS) LPAREN (STRING|IDENT) (COMMA (STRING|IDENT))* RPAREN
    | DOT RENAMELABELKEY LPAREN (STRING|IDENT) COMMA (STRING|IDENT) RPAREN
    | DOT RENAMELABELVALUE LPAREN (STRING|IDENT) COMMA (STRING|IDENT) (COMMA (STRING|IDENT))? RPAREN
    | DOT TIMECLIP LPAREN (NUMBER|IDENT) COMMA (NUMBER|IDENT) RPAREN
    | DOT TIMEMODULO LPAREN (NUMBER|IDENT) COMMA (STRING|IDENT) RPAREN
    | DOT TIMESPLIT LPAREN (NUMBER|DURATIONVAL) COMMA (NUMBER|IDENT) COMMA (STRING|IDENT) RPAREN
    ;

// Apply a multiple operations between several statements results
opExpr : operator LPAREN expr ( COMMA expr )+ RPAREN DOT limitOperatorList seriesOperations*
    | operator LPAREN expr ( COMMA expr )+ RPAREN seriesOperations*
    ;

// Valid operator than can be applied on multiple Time series
operator: ( ADDSERIES | ANDL | DIVSERIES | EQUAL | GREATEROREQUAL | GREATERTHAN | LESSOREQUAL | LESSTHAN | MULSERIES | NOTEQUAL | ORL | SUBSERIES )
    ;

// Operator operation to compute equivalence class between several time series set
limitOperatorList : limitOperator LPAREN (STRING_LIST|EMPTY_LIST|STRING|IDENT) RPAREN groupLimitOperatorList*
    | limitOperator LPAREN RPAREN groupLimitOperatorList*
    ;

// Group method as computed in PromQL
groupLimitOperatorList : DOT groupLimitOperator LPAREN RPAREN
    ;

// Valid group methods
groupLimitOperator : ( GROUPLEFT | GROUPRIGHT)
    ;

// Valid limit operators to compute equivalence class
limitOperator : ( ON | IGNORING )
    ;

// Empty list type
EMPTY_LIST : '[' ']'
    ;

// STRING|IDENT list type
STRING_LIST : '[' WS* (STRING|IDENT) LIST_ITEMS* WS* ']'
    ;

// List with several elements
LIST_ITEMS : COMMA (STRING|IDENT)
    ;

//
// Whitespace and comments
////channel(HIDDEN)

WS  :  [ \t]+ -> skip 
    ;

COMMENT
    :   '/*' .*? '*/' -> skip
    ;

TERMINATOR
	: [\r\n]+ -> skip
	;


LINE_COMMENT
    :   '//' ~[\r\n]* -> skip
    ;
