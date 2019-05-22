package tsl

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var toPromQl = [...]string{
	MEAN:      "avg",
	ADDSERIES: "+",
	ANDL:      "and",
	ORL:       "or",

	SUBSERIES:      "-",
	MULSERIES:      "*",
	DIVSERIES:      "/",
	EQUAL:          "==",
	NOTEQUAL:       "!=",
	PERCENTILE:     "quantile",
	GREATEROREQUAL: ">=",
	GREATERTHAN:    ">",
	LESSOREQUAL:    "<=",
	LESSTHAN:       "<",
	EqualMatch:     "=",
	NotEqualMatch:  "!=",
	RegexMatch:     "=~",
	RegexNoMatch:   "!~",
	WEEKDAY:        "day_of_week",
	DAY:            "day_of_month",
	MAXWITH:        "clamp_min",
	MINWITH:        "clamp_max",
	SORTDESC:       "sort_desc",
	TOPN:           "topk",
	BOTTOMN:        "bottomk",
}

// Ql main syntax
type Ql struct {
	API          string `json:"api,omitempty"`
	Token        string `json:"token,omitempty"`
	Query        string `json:"query,omitempty"`
	InstantQuery bool   `json:"instantQuery,omitempty"`
	Start        string `json:"start,omitempty"`
	End          string `json:"end,omitempty"`
	Step         string `json:"step,omitempty"`
}

// GeneratePromQl Generate Global Promql to execute from an instruction list
func (protoParser *ProtoParser) GeneratePromQl(instruction Instruction, now time.Time) (*Ql, error) {

	promql := &Ql{}

	if instruction.hasSelect {
		return protoParser.promSelectQuery(instruction, now)
	} else if instruction.isGlobalOperator {

		gOp := instruction.globalOperator

		joiner := " " + toPromQl[gOp.operator]

		// By default prom compute operator on all labels, add ignoring only when all isn't set
		if len(gOp.ignoring) > 0 {
			labels := protoParser.getOnLabels(gOp.ignoring, IGNORING.String(), gOp.group, gOp.groupLabels)
			joiner = joiner + " " + labels
		} else if len(gOp.labels) > 0 {
			labels := protoParser.getOnLabels(gOp.labels, ON.String(), gOp.group, gOp.groupLabels)
			joiner = joiner + " " + labels
		}
		joiner = joiner + " "
		for index, gOpInstruction := range gOp.instructions {

			internalQl, err := protoParser.GeneratePromQl(*gOpInstruction, now)
			if err != nil {
				return nil, err
			}

			if index == 0 {
				promql = internalQl
			} else {
				if !(promql.Step == internalQl.Step && promql.Start == internalQl.Start && promql.End == internalQl.End) {
					message := "expects same time properties for each metrics selector of an operator at method " + gOp.operator.String()
					return nil, protoParser.NewProtoError(message, gOp.pos)
				}
				promql.Query = promql.Query + joiner + internalQl.Query
			}
		}

		if len(instruction.selectStatement.frameworks) > 0 {

			var err error
			promql.Query, promql.InstantQuery, err = protoParser.promFrameworksOp(instruction.selectStatement.frameworks, promql, true)

			if err != nil {
				return nil, err
			}
		}

	} else {
		promql.Query = ""
	}

	return promql, nil
}

// Generate labels list after an operator
func (protoParser *ProtoParser) getOnLabels(labels []string, operator string, group InternalField, groupLabels []string) string {
	var buffer bytes.Buffer
	buffer.WriteString(operator + "(")

	prefix := ""
	for _, label := range labels {
		buffer.WriteString(prefix)
		buffer.WriteString(label)
		prefix = ","
	}
	buffer.WriteString(")")

	// When series has group labels
	if group.lit != "" {
		buffer.WriteString(" " + group.lit)

		if len(groupLabels) > 0 {

			buffer.WriteString("(")
			prefix := ""
			for _, label := range groupLabels {

				buffer.WriteString(prefix)
				buffer.WriteString(label)
				prefix = ","
			}
			buffer.WriteString(")")
		}
	}

	return buffer.String()
}

// Generate a select query in PromQL format
func (protoParser *ProtoParser) promSelectQuery(instruction Instruction, now time.Time) (*Ql, error) {
	var err error
	promql := &Ql{}

	if instruction.selectStatement.selectAll {
		message := "select all metrics not supported"
		return nil, protoParser.NewProtoError(message, instruction.selectStatement.pos)
	}

	promql.Query = instruction.selectStatement.metric
	promql.Token = instruction.connectStatement.token

	// Load default now
	end := float64(now.UnixNano()/int64(time.Millisecond)) / 1000.0
	hour := time.Hour
	then := now.Add(-hour)
	start := float64(then.UnixNano()/int64(time.Millisecond)) / 1000.0

	//start = float64(time.Now().UnixNano()/int64(time.Millisecond)) / 1000.0

	promql.End = strconv.FormatFloat(end, 'f', -1, 64)
	promql.Start = strconv.FormatFloat(start, 'f', -1, 64)
	promql.Step = "30s"

	if instruction.selectStatement.hasFrom {

		promql.Start = instruction.selectStatement.from.from.lit

		if instruction.selectStatement.from.hasTo {
			promql.End = instruction.selectStatement.from.to.lit
		}
	}

	if instruction.selectStatement.hasLast {

		if instruction.selectStatement.last.isDuration {
			lastValue := instruction.selectStatement.last.last
			userDuration, err := time.ParseDuration(lastValue)

			if err != nil {
				return nil, err
			}
			then := now.Add(-userDuration)
			start := float64(then.UnixNano()/int64(time.Millisecond)) / 1000.0
			promql.Start = strconv.FormatFloat(start, 'f', -1, 64)

		} else {

			message := "last supports only duration values in select statement"
			return nil, protoParser.NewProtoError(message, instruction.selectStatement.pos)
		}
	}

	findSample := false
	if len(instruction.selectStatement.frameworks) > 0 {
		if instruction.selectStatement.frameworks[0].operator == SAMPLEBY ||
			instruction.selectStatement.frameworks[0].operator == SAMPLE {
			findSample = true
			var err error
			promql.Step, err = protoParser.promSampleBy(instruction.selectStatement.frameworks[0])
			if err != nil {
				return nil, err
			}
		}
	}

	// All promQl expect a default sampleBy method
	if !findSample {
		message := "expects a default sample for each select statement"
		return nil, protoParser.NewProtoError(message, instruction.selectStatement.pos)
	}

	if len(instruction.selectStatement.where) > 0 {
		promql.Query = promql.Query + promWhereFields(promql.Query, instruction.selectStatement.where)
	}

	if len(instruction.selectStatement.frameworks) > 0 {
		promql.Query, promql.InstantQuery, err = protoParser.promFrameworksOp(instruction.selectStatement.frameworks, promql, false)
	}

	if err != nil {
		return nil, err
	}

	return promql, nil
}

// Load promQL where fields
func promWhereFields(query string, fields []WhereField) string {
	var buffer bytes.Buffer

	buffer.WriteString("{")

	prefix := ""
	for _, label := range fields {
		buffer.WriteString(prefix)
		newLabels := fmt.Sprintf(label.key+toPromQl[label.op]+"%q", label.value)
		buffer.WriteString(newLabels)
		prefix = ","
	}
	buffer.WriteString("}")

	return buffer.String()
}

// Load promQL sampleBy
func (protoParser *ProtoParser) promSampleBy(sampleBy FrameworkStatement) (string, error) {
	if agg, hasAggregator := sampleBy.attributes[SampleAggregator]; hasAggregator {
		if agg.tokenType != LAST {
			message := "steps are used to control query jump to sample data. Only the last aggregator is allowed in sampleBy methods"
			return "", protoParser.NewProtoError(message, sampleBy.pos)
		}
	}

	if _, hasSpan := sampleBy.attributes[SampleSpan]; !hasSpan {
		message := "sampling expects a sample span as duration value (1m) as first parameter"
		return "", protoParser.NewProtoError(message, sampleBy.pos)
	}
	return sampleBy.attributes[SampleSpan].lit, nil
}

// promFrameworksOp Generate each individual method statement, returns the PromQL query string and whether its a range_query (false by default) or an instant query
func (protoParser *ProtoParser) promFrameworksOp(frameworks []FrameworkStatement, promql *Ql, skipSample bool) (string, bool, error) {

	hasWindowMapper := false
	hasOffset := false
	offset := ""
	var buffer bytes.Buffer

	prefix := make([]string, 0)
	var suffix bytes.Buffer

	hasKeepLastValue := false

	for index, framework := range frameworks {
		// Skip first sample operator
		if index == 0 && (framework.operator == SAMPLE || framework.operator == SAMPLEBY) {
			continue
		}

		if hasKeepLastValue {
			message := "keepLastValues need to be the last method call on a Prometheus query"
			return "", hasKeepLastValue, protoParser.NewProtoError(message, framework.pos)
		}
		switch framework.operator {
		case SHIFT:
			if hasOffset {
				message := "shift can be done only once"
				return "", hasKeepLastValue, protoParser.NewProtoError(message, framework.pos)
			}

			hasOffset = true
			offset = framework.attributes[MapperValue].lit

		case SAMPLEBY, SAMPLE:
			if skipSample {
				continue
			}
			message := "sampling must be the first operation set"
			return "", hasKeepLastValue, protoParser.NewProtoError(message, framework.pos)

		case ADDSERIES, ANDL, SUBSERIES, MULSERIES, DIVSERIES, EQUAL, GREATEROREQUAL, GREATERTHAN, NOTEQUAL, LESSOREQUAL, LESSTHAN, ORL:
			suffixGroup, err := protoParser.promArithmeticOperators(framework)
			if err != nil {
				return "", hasKeepLastValue, err
			}

			suffix.WriteString(suffixGroup)

		case KEEPLASTVALUES:
			hasKeepLastValue = true
			if len(framework.attributes) > 0 {

				numberValue, err := strconv.Atoi(framework.attributes[MapperValue].lit)

				if numberValue > 1 || err != nil {
					message := "keepLastValues can't be applied with an argument as it call instant values query in Prometheus"
					return "", hasKeepLastValue, protoParser.NewProtoError(message, framework.pos)
				}
			}

		case MEAN, MIN, MAX, SUM, COUNT, STDDEV, STDVAR, RATE, DELTA, PERCENTILE, WINDOW:

			if hasWindowMapper {
				message := "over_time " + framework.operator.String() + " methods can be done only once per query"
				return "", hasKeepLastValue, protoParser.NewProtoError(message, framework.pos)
			}

			promStatement, err := protoParser.promOverTime(promql.Query, framework, hasOffset, offset, promql.Step)
			if err != nil {
				return "", hasKeepLastValue, err
			}
			buffer.WriteString(promStatement)
			hasWindowMapper = true

		case GROUPBY, GROUP, GROUPWITHOUT:
			promStatement, suffixGroup, err := protoParser.promGroup(framework)
			suffix.WriteString(suffixGroup)
			if err != nil {
				return "", hasKeepLastValue, err
			}
			prefix = append(prefix, promStatement)

		case ABS, DAY, LN, LOG2, LOG10, CEIL, FLOOR, ROUND, HOUR, MAXWITH, MINUTE, MINWITH, MONTH, SQRT, RESETS, TIMESTAMP, YEAR, WEEKDAY, SORT, SORTDESC, TOPN, BOTTOMN:
			promStatement, suffixGroup, err := protoParser.promOperator(framework)
			suffix.WriteString(suffixGroup)
			if err != nil {
				return "", hasKeepLastValue, err
			}

			// Append to request prefix
			prefix = append(prefix, promStatement)

		default:
			message := "operator " + framework.operator.String() + " not supported in TSL for " + protoParser.Name
			return "", hasKeepLastValue, protoParser.NewProtoError(message, framework.pos)
		}

	}

	// Reverse prefix string array
	last := len(prefix) - 1
	for i := 0; i < len(prefix)/2; i++ {
		prefix[i], prefix[last-i] = prefix[last-i], prefix[i]
	}

	// Returned sampled metrics
	if hasWindowMapper {
		resString := strings.Join(prefix, "") + buffer.String() + suffix.String()
		return resString, hasKeepLastValue, nil
	}

	// Returned shifted metrics
	if hasOffset {
		buffer.WriteString(promql.Query + " offset " + offset + ")")

		resString := strings.Join(prefix, "") + buffer.String() + suffix.String()
		return resString, hasKeepLastValue, nil
	}

	// Return operatored metrics
	resString := strings.Join(prefix, "") + promql.Query + suffix.String()
	return resString, hasKeepLastValue, nil
}

func (protoParser *ProtoParser) promArithmeticOperators(framework FrameworkStatement) (string, error) {
	operatorString := " " + toPromQl[framework.operator]

	value := ""

	if attribute, ok := framework.attributes[MapperValue]; ok {

		if framework.operator == ANDL || framework.operator == ORL {
			if attribute.tokenType != TRUE && attribute.tokenType != FALSE {
				message := "and or or operation works only using a boolean value"
				return "", protoParser.NewProtoError(message, framework.pos)
			}

		} else if attribute.tokenType != INTEGER && attribute.tokenType != NUMBER && attribute.tokenType != NEGINTEGER && attribute.tokenType != NEGNUMBER {
			message := "arithmetic operation works only on integer or number values"
			return "", protoParser.NewProtoError(message, framework.pos)
		}
		value = " " + attribute.lit
	}
	return operatorString + value, nil
}

func (protoParser *ProtoParser) promOperator(framework FrameworkStatement) (string, string, error) {
	// Set current operator
	operator := framework.operator.String()

	suffix := ")"
	prefix := "("

	switch framework.operator {
	case DAY, WEEKDAY, SORTDESC:
		operator = toPromQl[framework.operator]

	case MAXWITH, MINWITH:
		operator = toPromQl[framework.operator]
		suffix = "," + framework.attributes[MapperValue].lit + ")"

	case TOPN, BOTTOMN:
		operator = toPromQl[framework.operator]
		prefix = prefix + framework.attributes[NValue].lit + ","
	}

	if len(framework.unNamedAttributes) > 0 {
		groupOp := "by"
		if framework.operator == GROUPWITHOUT {
			groupOp = "without"
		}
		suffix = suffix + protoParser.promLabelsString(groupOp, framework.unNamedAttributes)
	}
	return operator + prefix, suffix, nil
}

func (protoParser *ProtoParser) promGroup(framework FrameworkStatement) (string, string, error) {
	// Set current operator
	operator := framework.attributes[Aggregator].lit

	switch framework.attributes[Aggregator].tokenType {
	case STRING:
		if framework.attributes[Aggregator].lit == MEAN.String() {
			operator = "avg"
		} else if framework.attributes[Aggregator].lit == PERCENTILE.String() {
			operator = "quantile"
		}
	case SUM, MIN, MAX, STDDEV, STDVAR, COUNT:
	case MEAN:
		operator = "avg"
	case PERCENTILE:
		operator = "quantile"
	default:
		message := "aggregator " + framework.attributes[Aggregator].tokenType.String() + " isn't valid"
		return "", "", protoParser.NewProtoError(message, framework.pos)
	}

	param := ""

	if operator == "quantile" {

		q, err := strconv.ParseFloat(framework.unNamedAttributes[0].lit, 64)

		if err != nil {
			message := "over_time function return an error when parsing percentile parameter"
			return "", "", protoParser.NewProtoError(message, framework.pos)
		}
		q = q / 100.0
		param = strconv.FormatFloat(q, 'f', -1, 64) + ","
		delete(framework.unNamedAttributes, 0)
	}

	suffix := ")"

	if len(framework.unNamedAttributes) > 0 {
		suffix = suffix + protoParser.promLabelsString("by", framework.unNamedAttributes)
	}
	return operator + "(" + param, suffix, nil
}

// getLabelsString generate a list of elements base on an Internal fields map
func (protoParser *ProtoParser) promLabelsString(group string, fields map[int]InternalField) string {
	var buffer bytes.Buffer

	buffer.WriteString(" " + group + " (")

	prefix := ""
	for _, label := range fields {
		buffer.WriteString(prefix)
		buffer.WriteString(label.lit)
		prefix = ", "
	}
	buffer.WriteString(")")

	return buffer.String()
}

func (protoParser *ProtoParser) promOverTime(query string, framework FrameworkStatement, hasShift bool, offset string, step string) (string, error) {

	// By default span equals step
	span := step

	// Set current over time operator
	aggregator := framework.operator.String()
	if framework.operator == MEAN || framework.operator == PERCENTILE {
		aggregator = toPromQl[framework.operator]
	}

	if framework.operator == WINDOW {
		aggregator = framework.attributes[Aggregator].tokenType.String()

		if aggregator == MEAN.String() {
			aggregator = toPromQl[MEAN]
		} else if aggregator == PERCENTILE.String() {
			aggregator = toPromQl[PERCENTILE]
		}
	}

	param := ""

	if framework.operator == PERCENTILE || aggregator == toPromQl[PERCENTILE] {
		q, err := strconv.ParseFloat(framework.attributes[MapperValue].lit, 64)

		if err != nil {
			message := "over_time function return an error when parsing percentile parameter "
			return "", protoParser.NewProtoError(message, framework.pos)
		}
		q = q / 100.0
		param = strconv.FormatFloat(q, 'f', -1, 64) + ","
	}

	functionName := aggregator + "_over_time(" + param

	if framework.operator == RATE {
		functionName = framework.operator.String() + "("
	}

	// Verify current mapper has only one parameter: a mapper sampling
	sampling, hasSampler := framework.attributes[MapperSampling]

	if framework.operator == WINDOW {
		sampling, hasSampler = framework.attributes[MapperPre]
	}

	if !hasSampler {
		if _, hasUnNamedAttributes := framework.unNamedAttributes[0]; !hasUnNamedAttributes {
			message := "over_time function expects one mapper sampling for " + framework.operator.String()
			return "", protoParser.NewProtoError(message, framework.pos)
		}
	}

	// Change span to match user one
	if hasSampler {
		span = sampling.lit
	}

	functionName = functionName + query + "[" + span + "]"

	if !hasShift {
		functionName = functionName + ")"
	} else {
		functionName = functionName + " offset " + offset + ")"
	}

	return functionName, nil
}
