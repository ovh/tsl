lexer grammar tslTokens; // note "TSL tokens"

// TSL native types and tokens
DURATIONVAL: INT_LIT ([wdhms]|'ms'|'us'|'ns'|'ps')
    ;

NUMBER: INT_LIT 
    | FLOAT_LIT
    ;

STRING: RAW_STRING_LIT
    | INTERPRETED_STRING_LIT
    ;

TRUE:        'TRUE' | 'true' | 'T' | 'True';
FALSE:       'FALSE' | 'false' | 'F' | 'False';

ASTERISK: '*';
EQ:       '=';
DIV:      '/';

LPAREN:      '(';
RPAREN:      ')';
COMMA:       ',';
SEMICOLON:   ';';
COLON:       ':';
DOUBLECOLON: '::';
DOT:         '.';

ABS:               'abs';
ADDNAMESUFFIX:     'addSuffix';
ADDNAMEPREFIX:     'addPrefix';
ADDSERIES:         'add';
ANDL:              'and';
ATTRIBUTEPOLICY:   'attributePolicy';
BOTTOMN:           'bottomN';
BOTTOMNBY:         'bottomNBy';
CEIL:              'ceil';
CONNECT:           'connect';
COUNT:             'count';
CUMULATIVE:        'cumulative';
CUMULATIVESUM:     'cumulativeSum';
DAY:               'day';
DELTA:             'delta';
DIVSERIES:         'div';
EQUAL:             'equal';
FILTERBYLABELS:    'filterByLabels';
FILTERBYNAME:      'filterByName';
FILTERBYLASTVALUE: 'filterByLastValue';
FIRST:             'first';
FLOOR:             'floor';
FROM:              'from';
GREATEROREQUAL:    'greaterOrEqual';
GREATERTHAN:       'greaterThan';
GROUP:             'group';
GROUPLEFT:         'groupLeft';
GROUPRIGHT:        'groupRight';
GROUPBY:           'groupBy';
GROUPWITHOUT:      'groupWithout';
HOUR:              'hour';
IGNORING:          'ignoring';
JOIN:              'join';
LAST:              'last';
LABELS:            'labels';
LESSOREQUAL:       'lessOrEqual';
LESSTHAN:          'lessThan';
LN:                'ln';
LOG2:              'log2';
LOG10:             'log10';
LOGN:              'logN';
MASK:              'mask';
MAX:               'max';
MAXWITH:           'maxWith';
MEAN:              'mean';
MEDIAN:            'median';
MIN:               'min';
MINWITH:           'minWith';
MINUTE:            'minute';
MONTH:             'month';
MULSERIES:         'mul';
NEGMASK:           'negmask';
NOTEQUAL:          'notEqual';
NAMES:             'names';
ON:                'on';
ORL:               'or';
PERCENTILE:        'percentile';
PROM:              'prom';
PROMETHEUS:        'prometheus';
RATE:              'rate';
REMOVELABELS:      'removeLabels';
REMOVE:            'remove';
RENAME:            'rename';
RENAMEBY:          'renameBy';
RENAMELABELKEY:    'renameLabelKey';
RENAMELABELVALUE:  'renameLabelValue';
RESETS:            'resets';
ROUND:             'round';
SAMPLE:            'sample';
SAMPLEBY:          'sampleBy';
SELECT:            'select';
SELECTORS:         'selectors';
SHIFT:             'shift';
SHRINK:            'shrink';
SORT:              'sort';
SORTBY:            'sortBy';
SORTDESC:          'sortDesc';
SORTDESCBY:        'sortDescBy';
SQRT:              'sqrt';
STDDEV:            'stddev';
STDVAR:            'stdvar';
STORE:             'store';
SUBSERIES:         'sub';
SUM:               'sum';
TOPN:              'topN';
TOPNBY:            'topNBy';
TIMECLIP:          'timeclip';
TIMEMODULO:        'timemodulo';
TIMESTAMP:         'timestamp';
TIMESCALE:         'timescale';
TIMESPLIT:         'timesplit';
WARP:              'warp10';
WEEKDAY:           'weekday';
WHERE:             'where';
WINDOW:            'window';
YEAR:              'year';

// Floating-point literals

//float_lit = decimals "." [ decimals ] [ exponent ] |
//            decimals exponent |
//            "." decimals [ exponent ] .
FLOAT_LIT
    : DECIMALS '.' DECIMALS? EXPONENT?
    | DECIMALS EXPONENT
    | '.' DECIMALS EXPONENT?
    ;

//int_lit     = decimal_lit | octal_lit | hex_lit .
INT_LIT
    : DECIMAL_LIT
    | OCTAL_LIT
    | HEX_LIT
    ;

//decimal_lit = ( "1" … "9" ) { decimal_digit } .
fragment DECIMAL_LIT
    : [1-9] DECIMAL_DIGIT*
    ;

//decimal_digit = "0" … "9" .
fragment DECIMAL_DIGIT
    : [0-9]
    ;
//exponent  = ( "e" | "E" ) [ "+" | "-" ] decimals .
fragment EXPONENT
    : ( 'e' | 'E' ) ( '+' | '-' )? DECIMALS
    ;

//decimals  = decimal_digit { decimal_digit } .
fragment DECIMALS
    : DECIMAL_DIGIT+
    ;

//octal_lit   = "0" { octal_digit } .
fragment OCTAL_LIT
    : '0' OCTAL_DIGIT*
    ;

//hex_lit     = "0" ( "x" | "X" ) hex_digit { hex_digit } .
fragment HEX_LIT
    : '0' ( 'x' | 'X' ) HEX_DIGIT+
    ;

fragment RAW_STRING_LIT
    : '`' ( UNICODE_CHAR | NEWLINE | [~`] )*? '`'
    ;

//unicode_char = /* an arbitrary Unicode code point except newline */ .
fragment UNICODE_CHAR   : ~[\u000A] ;

//newline = /* the Unicode code point U+000A */ .
fragment NEWLINE
    : [\u000A]
    ;

fragment INTERPRETED_STRING_LIT
    : ('"'|'\'') ( '\\"' | UNICODE_VALUE | BYTE_VALUE )*? ('"'|'\'')
    ;

//byte_value       = octal_byte_value | hex_byte_value .
fragment BYTE_VALUE
    : OCTAL_BYTE_VALUE | HEX_BYTE_VALUE
    ;

//hex_digit     = "0" … "9" | "A" … "F" | "a" … "f" .
fragment HEX_DIGIT
    : [0-9a-fA-F]
    ;

//octal_digit   = "0" … "7" .
fragment OCTAL_DIGIT
    : [0-7]
    ;

//octal_byte_value = `\` octal_digit octal_digit octal_digit .
fragment OCTAL_BYTE_VALUE
    : '\\' OCTAL_DIGIT OCTAL_DIGIT OCTAL_DIGIT
    ;


//hex_byte_value   = `\` "x" hex_digit hex_digit .
fragment HEX_BYTE_VALUE
    : '\\' 'x' HEX_DIGIT HEX_DIGIT
    ;

//little_u_value   = `\` "u" hex_digit hex_digit hex_digit hex_digit .
//                           hex_digit hex_digit hex_digit hex_digit .
LITTLE_U_VALUE
    : '\\u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;

//big_u_value      = `\` "U" hex_digit hex_digit hex_digit hex_digit
BIG_U_VALUE
    : '\\U' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;

//escaped_char     = `\` ( "a" | "b" | "f" | "n" | "r" | "t" | "v" | `\` | "'" | `"` ) .
fragment ESCAPED_CHAR
    : '\\' ( 'a' | 'b' | 'f' | 'n' | 'r' | 't' | 'v' | '\\' | '\'' | '"' )
    ;
//unicode_value    = unicode_char | little_u_value | big_u_value | escaped_char .
fragment UNICODE_VALUE
    : UNICODE_CHAR
    | LITTLE_U_VALUE
    | BIG_U_VALUE
    | ESCAPED_CHAR
    ;


//
// Source code representation
//
IDENT  :  (LETTER|'_') (LETTER|DIGIT|'_')*
    ;

NL      :   '\r'? '\n' ;

DIGIT:  '0'..'9' ; 
fragment LETTER  : [a-zA-Z] ;