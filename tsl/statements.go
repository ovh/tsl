package tsl

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// Statement represent one statement.
type Statement interface {
	String() string
}

// Query represents a collection of statements.
type Query struct {
	Statements Statements
}

// A Statements collections represents a list of instructions.
type Statements []*Instruction

// Instruction represents a complete set of TSL methods
type Instruction struct {
	connectStatement ConnectStatement
	selectStatement  SelectStatement
	createStatement  CreateStatement
	globalOperator   GlobalOperator
	isMeta           bool
	hasSelect        bool
	isGlobalOperator bool
	pos              Pos
}

// Variable represents a TSL variable
type Variable struct {
	name        string
	tokenType   Token
	lit         string
	instruction Instruction
	fieldList   []InternalField
}

// GlobalOperator represents an operation on a set of instruction in TSL
type GlobalOperator struct {
	operator     Token
	instructions Statements
	labels       []string
	ignoring     []string
	isIgnoring   bool
	isOn         bool
	group        InternalField
	groupLabels  []string
	pos          Pos
}

// ConnectStatement represents the CONNECT instruction
type ConnectStatement struct {
	connectType string
	api         string
	token       string
	pos         Pos
}

// CreateStatement represents a TSL create series instruction
type CreateStatement struct {
	createSeries []CreateSeries
	pos          Pos
}

// CreateSeries represents a new series data struct
type CreateSeries struct {
	metric    string
	selectAll bool
	where     []WhereField
	values    []DataPoint
	end       *InternalField
}

// DataPoint represents a new series data internal points
type DataPoint struct {
	tick  *InternalField
	value *InternalField
}

// SelectStatement represents a TSL SELECT instruction
type SelectStatement struct {
	metric          string
	selectAll       bool
	where           []WhereField
	last            LastStatement
	from            FromStatement
	hasFrom         bool
	hasLast         bool
	timeSet         bool
	frameworks      []FrameworkStatement
	hasRate         bool
	pos             Pos
	attributePolicy AttributePolicy
}

// WhereField correponds to an internal where field
type WhereField struct {
	key   string
	value string
	op    MatchType
}

// LastStatement represents the last method of the SELECT instruction
type LastStatement struct {
	last       string
	isDuration bool
	options    map[PrefixAttributes]string
	pos        Pos
}

// FromStatement represents the from part of the SELECT instruction
type FromStatement struct {
	from  InternalField
	to    InternalField
	hasTo bool
	pos   Pos
}

// FrameworkStatement represents a TSL framework operation done using a sampler, an operator or a groupBy
type FrameworkStatement struct {
	operator          Token
	attributes        map[PrefixAttributes]InternalField
	unNamedAttributes map[int]InternalField
	pos               Pos
}

// InternalField represents each method allowed field when parsing all instructions
type InternalField struct {
	tokenType     Token
	prefixName    PrefixAttributes
	hasPrefixName bool
	lit           string
	fieldList     []InternalField
}

// AttributePolicy is an enum to validate select attribute policy.
type AttributePolicy int

// All possibles fill policy
const (
	Merge AttributePolicy = iota
	Split
	Remove
)

// Fill policy to String method
func (m AttributePolicy) String() string {
	typeToStr := map[AttributePolicy]string{
		Merge:  "merge",
		Split:  "split",
		Remove: "remove",
	}
	if str, ok := typeToStr[m]; ok {
		return str
	}
	return ""
}

// FillPolicy is an enum to validate sampler fill policy.
type FillPolicy int

// All possibles fill policy
const (
	Next FillPolicy = iota
	Previous
	Interpolate
	None
	Auto
)

// Fill policy to String method
func (m FillPolicy) String() string {
	typeToStr := map[FillPolicy]string{
		Next:        "next",
		Previous:    "previous",
		Interpolate: "interpolate",
		None:        "none",
		Auto:        "auto",
	}
	if str, ok := typeToStr[m]; ok {
		return str
	}
	return ""
}

// MatchType is an enum for label matching types.
type MatchType int

// Possible MatchType
const (
	EqualMatch MatchType = iota
	NotEqualMatch
	RegexMatch
	RegexNoMatch
)

// Match type to String method
func (m MatchType) String() string {
	typeToStr := map[MatchType]string{
		EqualMatch:    "=",
		NotEqualMatch: "!=",
		RegexMatch:    "~",
		RegexNoMatch:  "!~",
	}
	if str, ok := typeToStr[m]; ok {
		return str
	}
	return ""
}

// StatementToString transform a list of statement into string
func StatementToString(statements []Statement) string {
	var buffer bytes.Buffer

	for _, v := range statements {
		buffer.WriteString(v.String())
	}
	return buffer.String()
}

//
// Statements string methods
//

func (q *Query) String() string { return q.Statements.String() }

func (a Statements) String() string {
	var str []string
	for _, stmt := range a {
		str = append(str, stmt.String())
	}
	return strings.Join(str, ";\n")
}

func (i Instruction) String() string {
	return i.connectStatement.String() + " " + i.selectStatement.String() + " " + i.globalOperator.String()
}

func (c ConnectStatement) String() string {
	return "ConnectStatement{ api: " + c.api + ", token: " + c.token + "fields: " + "}"
}

func (g GlobalOperator) String() string {
	statements := fmt.Sprintf("%q", g.instructions)
	labels := fmt.Sprintf("%q", g.labels)
	ignoring := fmt.Sprintf("%q", g.ignoring)
	return "GlobalOperator{ operator: " + g.operator.String() + ", instructions: " + statements + ", labels" + labels + ", ignoring" + ignoring + "}"
}

func (s SelectStatement) String() string {
	whereItems := fmt.Sprintf("%q", s.where)
	frameworks := fmt.Sprintf("%q", s.frameworks)
	return " SelectStatement{ metric: " + s.metric + "; where: " + whereItems + "; from: " + s.from.String() + "; last: " + s.last.String() + "; frameworks: " + frameworks + "; selectAll: " + strconv.FormatBool(s.selectAll) + "}"
}

func (where WhereField) String() string {
	return " WhereField{ key: " + where.key + "; op: " + where.op.String() + "; value: " + where.value + "}"
}

func (last LastStatement) String() string {
	options := fmt.Sprintf("%q", last.options)
	return " LastStatement{ last: " + last.last + "; isDuration: " + strconv.FormatBool(last.isDuration) + "; options: " + options + "}"
}

func (from FromStatement) String() string {
	fromItem := fmt.Sprintf("%q", from.from)
	toItem := fmt.Sprintf("%q", from.to)
	return " FromStatement{ from: " + fromItem + "; to: " + toItem + "}"
}

func (framework FrameworkStatement) String() string {
	attributes := AttributesToString(framework.attributes)
	return " FrameworkStatement{ op: " + framework.operator.String() + "; attributes: " + attributes + "}"
}

func (field InternalField) String() string {
	internalFields := fmt.Sprintf("%q", field.fieldList)
	return " InternalField{ Tok: " + field.tokenType.String() + "; lit: " + field.lit + "; Prefix: " + field.prefixName.String() + "internalFields: " + internalFields + " }"
}

// AttributesToString transform an Attributes map into string
func AttributesToString(attributes map[PrefixAttributes]InternalField) string {
	var buffer bytes.Buffer

	buffer.WriteString("{")
	prefix := ""
	for key, value := range attributes {
		buffer.WriteString(prefix)
		buffer.WriteString(key.String())
		buffer.WriteString(":")
		buffer.WriteString(value.String())
		prefix = ", "
	}
	buffer.WriteString("}")

	return buffer.String()
}
