package tsl

import (
	"strings"
)

// Token is a lexical token of the TSL language.
type Token int

// These are a comprehensive list of TSL language tokens.
const (
	// ILLEGAL Token, EOF, WS are Special TSL tokens.
	ILLEGAL Token = iota
	EOF
	WS
	COMMENT

	literalBeg
	// IDENT and the following are TSL literal tokens.
	IDENT                  // main
	BOUNDPARAM             // $param
	NUMBER                 // 12345.67
	INTEGER                // 12345TIMESPLIT
	NEGNUMBER              // NEGNUMBER
	NEGINTEGER             // NEGINTEGER
	DURATIONVAL            // 13h
	STRING                 // "abc"
	BADSTRING              // "abc
	BADESCAPE              // \q
	TRUE                   // true
	FALSE                  // false
	REGEX                  // Regular expressions
	BADREGEX               // `.*
	ASTERISK               // *
	EQ                     // =
	DIV                    //
	GTSLIST                // Internal GTS list type
	MULTIPLESERIESOPERATOR // Internal GTS list type
	literalEnd

	LPAREN       // (
	RPAREN       // )
	LBRACKET     // [
	RBRACKET     // ]
	COMMA        // ,
	COLON        // :
	DOUBLECOLON  // ::
	SEMICOLON    // ;
	DOT          // .
	INTERNALLIST // Fields list

	keywordBeg
	// ALL and the following are TSL Keywords
	ABS
	ADDNAMESUFFIX
	ADDNAMEPREFIX
	ADDSERIES
	ANDL
	ATTRIBUTEPOLICY
	BOTTOMN
	BOTTOMNBY
	CEIL
	CONNECT
	COUNT
	CUMULATIVE
	CUMULATIVESUM
	DAY
	DELTA
	DIVSERIES
	EQUAL
	FILTERBYLABELS
	FILTERBYNAME
	FILTERBYLASTVALUE
	FIRST
	FLOOR
	FROM
	GREATEROREQUAL
	GREATERTHAN
	GROUP
	GROUPLEFT
	GROUPRIGHT
	GROUPBY
	GROUPWITHOUT
	HOUR
	IGNORING
	JOIN
	LABELS
	LAST
	LESSOREQUAL
	LESSTHAN
	LN
	LOG2
	LOG10
	LOGN
	MASK
	MAX
	MAXWITH
	MEAN
	MEDIAN
	MIN
	MINWITH
	MINUTE
	MONTH
	MULSERIES
	NAMES
	NEGMASK
	NOTEQUAL
	ON
	ORL
	PERCENTILE
	PROM
	PROMETHEUS
	RATE
	REMOVE
	REMOVELABELS
	RENAME
	RENAMEBY
	RENAMELABELKEY
	RENAMELABELVALUE
	RESETS
	ROUND
	SAMPLE
	SAMPLEBY
	SELECT
	SELECTORS
	SHIFT
	SHRINK
	SORT
	SORTBY
	SORTDESC
	SORTDESCBY
	SQRT
	STDDEV
	STDVAR
	STORE
	SUBSERIES
	SUM
	TIMECLIP
	TIMEMODULO
	TIMESTAMP
	TIMESCALE
	TIMESPLIT
	TOPN
	TOPNBY
	WARP
	WEEKDAY
	WHERE
	WINDOW
	YEAR
	keywordEnd
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:       "IDENT",
	NUMBER:      "NUMBER",
	DURATIONVAL: "DURATIONVAL",
	STRING:      "STRING",
	BADSTRING:   "BADSTRING",
	BADESCAPE:   "BADESCAPE",
	TRUE:        "TRUE",
	FALSE:       "FALSE",
	REGEX:       "REGEX",
	GTSLIST:     "GTSLIST",

	ASTERISK: "*",
	EQ:       "=",
	DIV:      "/",

	LPAREN:      "(",
	RPAREN:      ")",
	COMMA:       ",",
	COLON:       ":",
	DOUBLECOLON: "::",
	SEMICOLON:   ";",
	DOT:         ".",

	ABS:               "abs",
	ADDNAMESUFFIX:     "addSuffix",
	ADDNAMEPREFIX:     "addPrefix",
	ADDSERIES:         "add",
	ANDL:              "and",
	ATTRIBUTEPOLICY:   "attributePolicy",
	BOTTOMN:           "bottomN",
	BOTTOMNBY:         "bottomNBy",
	CEIL:              "ceil",
	CONNECT:           "connect",
	COUNT:             "count",
	CUMULATIVE:        "cumulative",
	CUMULATIVESUM:     "cumulativeSum",
	DAY:               "day",
	DELTA:             "delta",
	DIVSERIES:         "div",
	EQUAL:             "equal",
	FILTERBYLABELS:    "filterByLabels",
	FILTERBYNAME:      "filterByName",
	FILTERBYLASTVALUE: "filterByLastValue",
	FIRST:             "first",
	FLOOR:             "floor",
	FROM:              "from",
	GREATEROREQUAL:    "greaterOrEqual",
	GREATERTHAN:       "greaterThan",
	GROUP:             "group",
	GROUPLEFT:         "groupLeft",
	GROUPRIGHT:        "groupRight",
	GROUPBY:           "groupBy",
	GROUPWITHOUT:      "groupWithout",
	HOUR:              "hour",
	IGNORING:          "ignoring",
	JOIN:              "join",
	LAST:              "last",
	LABELS:            "labels",
	LESSOREQUAL:       "lessOrEqual",
	LESSTHAN:          "lessThan",
	LN:                "ln",
	LOG2:              "log2",
	LOG10:             "log10",
	LOGN:              "logN",
	MASK:              "mask",
	MAX:               "max",
	MAXWITH:           "maxWith",
	MEAN:              "mean",
	MEDIAN:            "median",
	MIN:               "min",
	MINWITH:           "minWith",
	MINUTE:            "minute",
	MONTH:             "month",
	MULSERIES:         "mul",
	NEGMASK:           "negmask",
	NOTEQUAL:          "notEqual",
	NAMES:             "names",
	ON:                "on",
	ORL:               "or",
	PERCENTILE:        "percentile",
	PROM:              "prom",
	PROMETHEUS:        "prometheus",
	RATE:              "rate",
	REMOVELABELS:      "removeLabels",
	REMOVE:            "remove",
	RENAME:            "rename",
	RENAMEBY:          "renameBy",
	RENAMELABELKEY:    "renameLabelKey",
	RENAMELABELVALUE:  "renameLabelValue",
	RESETS:            "resets",
	ROUND:             "round",
	SAMPLE:            "sample",
	SAMPLEBY:          "sampleBy",
	SELECT:            "select",
	SELECTORS:         "selectors",
	SHIFT:             "shift",
	SHRINK:            "shrink",
	SORT:              "sort",
	SORTBY:            "sortBy",
	SORTDESC:          "sortDesc",
	SORTDESCBY:        "sortDescBy",
	SQRT:              "sqrt",
	STDDEV:            "stddev",
	STDVAR:            "stdvar",
	STORE:             "store",
	SUBSERIES:         "sub",
	SUM:               "sum",
	TOPN:              "topN",
	TOPNBY:            "topNBy",
	TIMECLIP:          "timeclip",
	TIMEMODULO:        "timemodulo",
	TIMESTAMP:         "timestamp",
	TIMESCALE:         "timescale",
	TIMESPLIT:         "timesplit",
	WARP:              "warp10",
	WEEKDAY:           "weekday",
	WHERE:             "where",
	WINDOW:            "window",
	YEAR:              "year",
}

var keywords map[string]Token

func init() {
	keywords = make(map[string]Token)
	for tok := keywordBeg + 1; tok < keywordEnd; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	keywords["true"] = TRUE
	keywords["false"] = FALSE
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

// tokstr returns a literal if provided, otherwise returns the token string.
func tokstr(tok Token, lit string) string {
	if lit != "" {
		return lit
	}
	return tok.String()
}

// Lookup returns the token associated with a given string.
func Lookup(ident string) Token {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok
	}
	return IDENT
}

// Pos specifies the line and character position of a token.
// The Char and Line are both zero-based indexes.
type Pos struct {
	Line int
	Char int
}

// PrefixAttributes is an enum for sampler fill policy.
type PrefixAttributes int

// Valid prefix attributes in TSL methods
const (
	FromFrom PrefixAttributes = iota
	FromTo
	LastShift
	LastTimestamp
	LastDate
	SampleRelative
	SampleFill
	SampleAuto
	SampleAggregator
	SampleSpan
	MapperValue
	MapperPre
	MapperPost
	MapperSampling
	MapperOccurences
	Aggregator
	NValue
	KeepDistinct
	GroupIsWithout
)

func (m PrefixAttributes) String() string {
	typeToStr := map[PrefixAttributes]string{
		FromFrom:         "from",
		FromTo:           "to",
		LastShift:        "shift",
		LastTimestamp:    "timestamp",
		LastDate:         "date",
		SampleRelative:   "relative",
		SampleFill:       "fill",
		SampleAggregator: "aggregator",
		SampleSpan:       "span",
		SampleAuto:       "count",
		MapperValue:      "mapperValue",
		MapperPre:        "pre",
		MapperPost:       "post",
		MapperSampling:   "sampler",
		MapperOccurences: "occurrences",
		Aggregator:       "aggregator",
		KeepDistinct:     "keepDistinct",
		NValue:           "n",
	}
	if str, ok := typeToStr[m]; ok {
		return str
	}
	return ""
}
