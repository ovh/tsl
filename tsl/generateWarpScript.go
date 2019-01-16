package tsl

import (
	"bytes"
	"fmt"
	"strings"
)

const (
	sampleAggregator = "mean"
	sampleAuto       = "30"
	sampleShiftSpan  = "0"
)

var toWarpScript = [...]string{
	RENAME:         "RENAME",
	RENAMEBY:       "RENAME",
	SHIFT:          "TIMESHIFT",
	SHRINK:         "SHRINK",
	TIMESCALE:      "TIMESCALE",
	TIMECLIP:       "TIMECLIP",
	TIMEMODULO:     "TIMEMODULO",
	TIMESPLIT:      "TIMESPLIT FLATTEN",
	STORE:          "UPDATE",
	RESETS:         "FALSE RESETS",
	DAY:            "'UTC' mapper.day",
	MINUTE:         "'UTC' mapper.minute",
	HOUR:           "'UTC' mapper.hour",
	MONTH:          "'UTC' mapper.month",
	WEEKDAY:        "'UTC' mapper.weekday",
	MAXWITH:        "mapper.max.x",
	MINWITH:        "mapper.min.x",
	YEAR:           "'UTC' mapper.year",
	STDDEV:         "TRUE mapper.sd",
	STDVAR:         "TRUE mapper.var",
	TIMESTAMP:      "mapper.tick",
	LN:             "e mapper.log",
	LOG2:           "2 mapper.log",
	LOG10:          "10 mapper.log",
	LOGN:           "mapper.log",
	BOTTOMNBY:      "SORTBY",
	SORTBY:         "SORTBY",
	SORTDESCBY:     "SORTBY REVERSE",
	TOPNBY:         "SORTBY REVERSE",
	BOTTOMN:        "SORTBY",
	SORT:           "SORTBY",
	SORTDESC:       "SORTBY REVERSE",
	TOPN:           "SORTBY REVERSE",
	EQUAL:          "eq",
	GREATEROREQUAL: "ge",
	GREATERTHAN:    "gt",
	LESSOREQUAL:    "le",
	LESSTHAN:       "lt",
	NOTEQUAL:       "ne",
	MEAN:           "mean",
}

// GenerateWarpScript Generate Global WarpScript to execute from an instruction list
func (protoParser *ProtoParser) GenerateWarpScript(instructions []Instruction, allowAuthenticate bool) (string, error) {
	var buffer bytes.Buffer

	buffer.WriteString("NOW 'now' STORE")
	buffer.WriteString("\n")

	// In case stack authentication is allowed in configuration
	if allowAuthenticate {

		// Search for a token in all queries
		for _, instruction := range instructions {

			// Apply authenticate on first token found
			if instruction.hasSelect && instruction.connectStatement.token != "" {

				// Authenticate the stack
				buffer.WriteString("'" + instruction.connectStatement.token + "' AUTHENTICATE")
				buffer.WriteString("\n")

				// Raise maxops and fetched DP limits
				buffer.WriteString("'stack.maxops.hard' STACKATTRIBUTE DUP <% ISNULL ! %> <% MAXOPS %> <% DROP %> IFTE")
				buffer.WriteString("\n")
				buffer.WriteString("'fetch.limit.hard' STACKATTRIBUTE DUP <% ISNULL ! %> <% LIMIT %> <% DROP %> IFTE")
				buffer.WriteString("\n")

				// Stop the loop
				break
			}
		}
	}

	for _, instruction := range instructions {

		warpScript, err := protoParser.processWarpScriptInstruction(instruction, "")
		if err != nil {
			return "", err
		}
		buffer.WriteString(warpScript)
		buffer.WriteString("\n")
	}
	return buffer.String(), nil
}

func (protoParser *ProtoParser) processWarpScriptInstruction(instruction Instruction, prefix string) (string, error) {
	var buffer bytes.Buffer

	// As Meta is a single instruction test it first and propagate result
	if instruction.isMeta {
		val, err := protoParser.getMeta(instruction.selectStatement, instruction.connectStatement.token)
		if err != nil {
			return "", err
		}
		return val, nil
	}

	if instruction.hasSelect {
		fetch, err := protoParser.getFetch(instruction.selectStatement, instruction.connectStatement.token, prefix)
		if err != nil {
			return "", nil
		}
		buffer.WriteString(prefix)
		buffer.WriteString(fetch)
		buffer.WriteString("\n")

		op, err := protoParser.getFrameworksOp(instruction.selectStatement, prefix)

		if err != nil {
			return "", err
		}
		buffer.WriteString(op)
	} else if instruction.isGlobalOperator {

		gOp, err := protoParser.writeGlobalOperators(instruction.globalOperator, prefix, instruction.selectStatement)
		if err != nil {
			return "", err
		}
		buffer.WriteString(gOp)

	}
	return buffer.String(), nil
}

// Write recursive operators
func (protoParser *ProtoParser) writeGlobalOperators(gOp GlobalOperator, prefix string, selectStatement SelectStatement) (string, error) {
	var buffer bytes.Buffer

	buffer.WriteString(prefix)

	if gOp.group.lit != "" {

		message := "TSL doesn't allow " + gOp.group.tokenType.String() + "methods"
		return "", protoParser.NewProtoError(message, gOp.pos)
	}

	// When all is set, use all existing labels to generate equivalence class
	if gOp.isIgnoring {
		buffer.WriteString("[] 'operatorLabels' STORE\n")
		buffer.WriteString(prefix)
	}

	buffer.WriteString("[ \n")

	for _, gOpInstruction := range gOp.instructions {

		warpScript, err := protoParser.processWarpScriptInstruction(*gOpInstruction, prefix+"  ")
		if err != nil {
			return "", err
		}
		buffer.WriteString(warpScript)

		if gOp.isIgnoring {
			buffer.WriteString(prefix + "  ")
			buffer.WriteString("DUP  <% DROP LABELS KEYLIST %> LMAP $operatorLabels APPEND 'operatorLabels' STORE\n")
		}
	}

	if gOp.isIgnoring {
		buffer.WriteString(prefix + "  ")
		buffer.WriteString("$operatorLabels FLATTEN UNIQUE \n")
		if len(gOp.ignoring) > 0 {
			labelsIgnoring := protoParser.getLabelsListString(gOp.ignoring)
			buffer.WriteString(prefix + "  ")
			buffer.WriteString("->SET ")
			buffer.WriteString(labelsIgnoring)
			buffer.WriteString(" ->SET DIFFERENCE SET-> \n")
		}
	} else {
		buffer.WriteString(prefix + "  ")
		labels := protoParser.getLabelsListString(gOp.labels)
		buffer.WriteString(labels + " \n")
	}
	buffer.WriteString(prefix + "  ")

	operator := "op."

	switch gOp.operator {
	case EQUAL, GREATEROREQUAL, GREATERTHAN, LESSOREQUAL, LESSTHAN, NOTEQUAL:
		operator += toWarpScript[gOp.operator]
	default:
		operator += gOp.operator.String()
	}
	buffer.WriteString(operator + " \n")
	buffer.WriteString(prefix)
	buffer.WriteString("] \n")
	buffer.WriteString(prefix)
	buffer.WriteString("APPLY \n")
	op, err := protoParser.getFrameworksOp(selectStatement, prefix)

	if err != nil {
		return "", err
	}
	buffer.WriteString(op)
	return buffer.String(), nil
}

// Generate each individual method statement
func (protoParser *ProtoParser) getFrameworksOp(selectStatement SelectStatement, prefix string) (string, error) {
	var buffer bytes.Buffer

	var sampleSpan string

	for _, framework := range selectStatement.frameworks {

		buffer.WriteString(prefix)
		switch framework.operator {
		case SAMPLEBY, SAMPLE:
			var bucketize string
			var err error
			bucketize, sampleSpan, err = protoParser.getBucketize(selectStatement, framework, prefix)
			if err != nil {
				return "", err
			}
			buffer.WriteString(bucketize)
			buffer.WriteString("\n")
		case ABS, ADDSERIES, ANDL, CEIL, COUNT, DAY, DELTA, DIVSERIES, EQUAL, FLOOR, GREATERTHAN, GREATEROREQUAL, LESSTHAN, LESSOREQUAL,
			LN, LOG2, LOG10, LOGN, HOUR, MAX, MAXWITH, MEAN, MEDIAN, MIN, MINWITH, MINUTE, MONTH, MULSERIES, NOTEQUAL, ORL, RATE, STDDEV, STDVAR,
			ROUND, SQRT, SUM, TIMESTAMP, WEEKDAY, YEAR, JOIN, PERCENTILE, CUMULATIVE, WINDOW, FINITE:

			buffer.WriteString(protoParser.getMapper(framework, sampleSpan))
			buffer.WriteString("\n")

		case SHIFT, RESETS, TIMESCALE, TIMECLIP, TIMEMODULO, TIMESPLIT, SHRINK:
			buffer.WriteString(protoParser.operators(framework))
			buffer.WriteString("\n")

		case KEEPLASTVALUES, KEEPFIRSTVALUES:
			keepValues, err := protoParser.parseKeepValues(framework)
			if err != nil {
				return "", err
			}
			buffer.WriteString(keepValues)
			buffer.WriteString("\n")

		case RENAME, STORE:
			buffer.WriteString(protoParser.nValuesOperators(framework))
			buffer.WriteString("\n")

		case FILTERBYLABELS, FILTERBYNAME, FILTERBYLASTVALUE:
			filter, err := protoParser.filterWarpScript(framework)
			if err != nil {
				return "", err
			}
			buffer.WriteString(filter)
			buffer.WriteString("\n")

		case ADDNAMESUFFIX:
			framework.operator = RENAME

			value := framework.unNamedAttributes[0]
			value.lit = "%2B" + value.lit
			framework.unNamedAttributes[0] = value

			buffer.WriteString(protoParser.nValuesOperators(framework))
			buffer.WriteString("\n")

		case ADDNAMEPREFIX:
			name, err := protoParser.addNamePrefix(framework)
			if err != nil {
				return "", err
			}
			buffer.WriteString(name)
			buffer.WriteString("\n")

		case RENAMEBY:
			rename, err := protoParser.renameBy(framework)
			if err != nil {
				return "", err
			}
			buffer.WriteString(rename)
			buffer.WriteString("\n")

		case REMOVELABELS:
			remove, err := protoParser.removeLabels(framework)
			if err != nil {
				return "", err
			}
			buffer.WriteString(remove)
			buffer.WriteString("\n")

		case RENAMELABELKEY:
			rename, err := protoParser.renameLabelKey(framework)
			if err != nil {
				return "", err
			}
			buffer.WriteString(rename)
			buffer.WriteString("\n")

		case RENAMELABELVALUE:
			rename, err := protoParser.renameLabelValue(framework)
			if err != nil {
				return "", err
			}
			buffer.WriteString(rename)
			buffer.WriteString("\n")

		case CUMULATIVESUM:
			framework.attributes[MapperPre] = InternalField{lit: "max.tick.sliding.window", tokenType: INTEGER}
			framework.attributes[MapperPost] = InternalField{lit: "0", tokenType: INTEGER}
			framework.operator = SUM
			buffer.WriteString(protoParser.getMapper(framework, sampleSpan))
			buffer.WriteString("\n")

		case BOTTOMNBY, SORTBY, SORTDESCBY, TOPNBY, BOTTOMN, SORT, SORTDESC, TOPN:
			opBy, err := protoParser.operatorBy(framework, true)
			if err != nil {
				return "", err
			}
			buffer.WriteString(opBy)
			buffer.WriteString("\n")

		case GROUPBY, GROUP, GROUPWITHOUT:
			buffer.WriteString(protoParser.getReducer(framework, prefix))
			buffer.WriteString("\n")
		}

	}
	return buffer.String(), nil
}

func (protoParser ProtoParser) getMeta(selectStatement SelectStatement, token string) (string, error) {

	for _, framework := range selectStatement.frameworks {
		switch framework.operator {
		case NAMES, SELECTORS, LABELS:
			return protoParser.getFind(selectStatement, token, framework)
		}
	}

	message := "unvalid meta operators in select statement"
	return "", protoParser.NewProtoError(message, selectStatement.pos)

}

func (protoParser ProtoParser) getFind(selectStatement SelectStatement, token string, framework FrameworkStatement) (string, error) {

	op := ""

	switch framework.operator {
	case NAMES:
		op = "NAME"
	case SELECTORS:
		op = "TOSELECTOR"
	case LABELS:
		op = "LABELS"
	}

	metric := selectStatement.metric
	if selectStatement.selectAll {
		metric = "~.*"
	}
	suffix := ""

	if framework.operator == LABELS && len(framework.unNamedAttributes) > 0 {
		suffix = "'" + framework.unNamedAttributes[0].lit + "' GET"
	}

	// Otherwise return last tick and duration from value in Fetch
	find := fmt.Sprintf("[ %q %q "+protoParser.getFetchLabels(selectStatement.where)+" ] FIND", token, metric)
	find += "\n<% DROP " + op + " " + suffix + " %> LMAP UNIQUE"
	return find, nil
}

// Generate a fetch statement
func (protoParser *ProtoParser) operatorBy(framework FrameworkStatement, hasBy bool) (string, error) {

	aggregator := "mean"

	attribute, hasAggregator := framework.attributes[Aggregator]

	// Aggretor must be set otherwise bucket wasn't validated
	if hasAggregator {
		aggregator = attribute.lit
	}

	value := ""

	if attribute, hasValue := framework.attributes[NValue]; hasValue {
		value = " [ 0 " + attribute.lit + " 1 - ] SUBLIST"
	}

	if !hasAggregator && (framework.operator == TOPN || framework.operator == BOTTOMN) {
		prefix := ""
		if framework.operator == TOPN {
			prefix = " REVERSE"
		}

		return prefix + value, nil
	}

	bucketizer := "bucketizer." + aggregator

	byMacro := "<% [ SWAP " + bucketizer + " 0 0 1 ] BUCKETIZE VALUES 0 GET 0 GET %> "

	operatorString := toWarpScript[framework.operator]

	return byMacro + operatorString + value, nil
}

// Generate a fetch statement
func (protoParser *ProtoParser) getFetch(selectStatement SelectStatement, token string, prefix string) (string, error) {

	lastTick, err := protoParser.getLastTick(selectStatement)
	if err != nil {
		return "", err
	}
	from := protoParser.getFrom(selectStatement)

	metric := selectStatement.metric
	if selectStatement.selectAll {
		metric = "~.*"
	}

	// Return find when no last or from methods were sets
	if !selectStatement.hasFrom && !selectStatement.hasLast {
		find := fmt.Sprintf("[ %q %q "+protoParser.getFetchLabels(selectStatement.where)+" ] FIND", token, metric)
		attPolicy := protoParser.getAttributePolicyString(selectStatement.attributePolicy, prefix)
		return find + attPolicy, nil
	}

	// When has from set return duration between from and lastitck
	if selectStatement.hasFrom {
		fetch := fmt.Sprintf("[ %q %q "+protoParser.getFetchLabels(selectStatement.where)+" "+from+" "+lastTick+" ] FETCH", token, metric)
		attPolicy := protoParser.getAttributePolicyString(selectStatement.attributePolicy, prefix)
		return fetch + attPolicy, nil
	}

	// Otherwise return last tick and duration from value in Fetch
	fetch := fmt.Sprintf("[ %q %q "+protoParser.getFetchLabels(selectStatement.where)+" "+lastTick+" "+from+" ] FETCH", token, metric)
	attPolicy := protoParser.getAttributePolicyString(selectStatement.attributePolicy, prefix)
	return fetch + prefix + attPolicy, nil
}

func (protoParser *ProtoParser) getAttributePolicyString(attPolicy AttributePolicy, prefix string) string {

	switch attPolicy {
	case Merge:
		return "\n" + prefix + "<% DROP DUP DUP ATTRIBUTES SWAP LABELS APPEND RELABEL %> LMAP"
	case Split:
		return ""
	case Remove:
		return "\n" + prefix + "<% DROP DUP ATTRIBUTES { SWAP <% DROP '' %> FOREACH } SETATTRIBUTES %> LMAP"
	}
	return ""
}

// Generate a bucketize statement
func (protoParser *ProtoParser) getBucketize(selectStatement SelectStatement, framework FrameworkStatement, prefix string) (string, string, error) {

	aggregator := sampleAggregator
	auto := sampleAuto

	bucketizerParams := ""
	// Load aggregator or by default use mean
	if attribute, ok := framework.attributes[SampleAggregator]; ok {
		aggregator = attribute.lit

		if attribute.tokenType == JOIN || attribute.tokenType == PERCENTILE {
			if framework.unNamedAttributes[0].tokenType == STRING {
				bucketizerParams = "'" + framework.unNamedAttributes[0].lit + "' "
			} else {
				bucketizerParams = framework.unNamedAttributes[0].lit + " "
			}
		}
	}
	bucketizer := bucketizerParams + "bucketizer." + aggregator

	// Same for span
	var shiftSpan string
	if attribute, ok := framework.attributes[SampleSpan]; ok {
		shiftSpan = protoParser.parseShift(attribute.lit)
	} else {
		// set default shift span to 0
		shiftSpan = sampleShiftSpan
	}

	hasCount := false
	// Check if current attributes has a count field otherwise use 30 as default value
	if attribute, ok := framework.attributes[SampleAuto]; ok {
		auto = attribute.lit
		hasCount = true
	}

	if framework.operator == SAMPLE {
		if selectStatement.hasLast && selectStatement.last.isDuration {
			shiftSpan = protoParser.getFrom(selectStatement) + " " + auto + " /"
		} else if selectStatement.hasFrom {
			if selectStatement.from.hasTo && (selectStatement.from.from.lit == selectStatement.from.to.lit) {
				return "", sampleShiftSpan, nil
			}
			// err previously catched
			last, _ := protoParser.getLastTimestamp(selectStatement)
			_, from := protoParser.getFromSampling(selectStatement)
			shiftSpan = last + " " + from + " - " + auto + " /"
		} else {
			message := "unexpected dates when parsing sample function"
			return "", "", protoParser.NewProtoError(message, framework.pos)
		}
	}

	if hasCount && shiftSpan == sampleShiftSpan && auto == sampleShiftSpan {
		message := "sampling expects at least a span or a count not equals to zero"
		return "", "", protoParser.NewProtoError(message, framework.pos)
	}

	// Get the select last bucket as timestamp
	lasttick, err := protoParser.getLastTimestamp(selectStatement)
	if err != nil {
		return "", "", err
	}

	// Load relative if it exists or use default set as true
	relative := true
	if attribute, ok := framework.attributes[SampleRelative]; ok {
		relative = attribute.tokenType == TRUE
	}

	relativeLastBucket := ""
	lastbucket := lasttick
	bucketizePrefix := "'raw' STORE "
	// Load relative if it exists or use default set as true
	if relative && shiftSpan != sampleShiftSpan {

		bucketizePrefix = bucketizePrefix + "$raw " + lasttick + " " + lasttick + " " + lasttick + " " + shiftSpan + " / " + shiftSpan + " * - TIMECLIP NONEMPTY \n" + prefix + "<% SIZE 0 > %>\n"

		relativeSpan := shiftSpan

		// When a count is set try to compute a relative span
		if shiftSpan == sampleShiftSpan {

			var err error
			relativeSpan, err = protoParser.getShift(selectStatement, auto)

			if err != nil {
				return "", "", nil
			}
		}

		// If relative span is still zero, compute absolute sample
		if relativeSpan != sampleShiftSpan {
			relativeLastBucket = lasttick + " " + relativeSpan + " / " + relativeSpan + " * "
			lastbucket = lasttick + " " + relativeSpan + " / " + relativeSpan + " * " + relativeSpan + " +"
		}
	}

	// Load fill policies
	fillPolicies := Auto.String()
	fillText := ""
	if attribute, ok := framework.attributes[SampleFill]; ok {
		fillPolicies = attribute.lit

		// When field is a list add each single fields
		if attribute.tokenType == INTERNALLIST {
			prefix := ""
			for _, internalField := range attribute.fieldList {
				fillText = fillText + prefix + protoParser.getPolicy(internalField.lit)
				prefix = " "
			}
		} else {
			fillText = protoParser.getPolicy(fillPolicies)
		}
	} else {

		// If no policy specified and count equals to 1, switch policy to None
		if hasCount && auto == "1" {
			fillPolicies = None.String()
		}
		fillText = protoParser.getPolicy(fillPolicies)
	}

	if fillValue, ok := framework.attributes[SampleFillValue]; ok {
		fillText = " [ NaN NaN NaN " + fillValue.lit + " ] FILLVALUE"
	}

	if !hasCount {
		var isFrom bool
		isFrom, auto = protoParser.getFromSampling(selectStatement)

		// In case it's a select between 2 dates compute duration between start and end date
		if isFrom {

			// Get the select last bucket as timestamp
			lastTimestamp, _ := protoParser.getLastTimestamp(selectStatement)
			auto = lastTimestamp + " " + auto + " - "
		}

		// auto is now a duration, dividing it by sampling span gives total bucket count value
		if auto != "0" {
			auto = auto + " " + shiftSpan + " /"
		}
	}

	bucketize := fmt.Sprintf("[ $raw " + bucketizer + " " + lastbucket + " " + shiftSpan + " " + auto + " ] BUCKETIZE " + fillText + " UNBUCKETIZE")

	if relative && shiftSpan != sampleShiftSpan {

		if selectStatement.hasRate {
			bucketize = "[ $raw " + bucketizer + " " + lastbucket + " " + shiftSpan + " " + auto + " ] BUCKETIZE \n"
			bucketize = bucketize + prefix + "     <% DROP 'series' STORE\n"
			bucketize = bucketize + prefix + "     $series DUP LASTBUCKET $series BUCKETSPAN - ATTICK DUP 0 GET 't0' STORE 4 GET 'v0' STORE\n"
			bucketize = bucketize + prefix + "     $series DUP LASTBUCKET $series BUCKETSPAN 2 * - ATTICK DUP 0 GET 't1' STORE 4 GET 'v1' STORE\n"
			bucketize = bucketize + prefix + "       <% $series SIZE 2 > $v0 ISNULL ! $v1 ISNULL ! && && %>\n"
			bucketize = bucketize + prefix + "       <%  $v0 DUP $v1 - TODOUBLE  $t0 $t1 - TODOUBLE / " + lasttick + " DUP " + shiftSpan + " / " + shiftSpan + " * - * + 'value' STORE\n"
			bucketize = bucketize + prefix + "           $series DUP  LASTBUCKET NaN NaN NaN $value SETVALUE %>\n"
			bucketize = bucketize + prefix + "       <% $series %> IFTE %> LMAP " + fillText + " UNBUCKETIZE"
		}

		// If has value in new area
		bucketize = "  <% " + bucketize + " %> \n"

		// Otherwise complete bucketize only on previous relative part
		bucketize = bucketize + prefix + "  <% [ $raw " + bucketizer + " " + relativeLastBucket + " " + shiftSpan + " " + auto + " ] BUCKETIZE " + fillText + " UNBUCKETIZE %> IFTE\n"

		// Reset lasttick to end
		bucketize += prefix + "<% DROP DUP DUP LASTTICK 'tick' STORE <% SIZE 0 > $tick " + lasttick + " > && %> <%  DUP $tick ATTICK 4 GET 'value' STORE DUP SIZE 1 - SHRINK " + lasttick + " NaN NaN NaN $value SETVALUE %> IFT %> LMAP"

	}

	return bucketizePrefix + bucketize, shiftSpan, nil
}

func (protoParser *ProtoParser) getPolicy(fillPolicies string) string {
	fillText := ""
	switch fillPolicies {
	case Auto.String():
		fillText = "INTERPOLATE FILLPREVIOUS FILLNEXT"
	case None.String():
		fillText = ""
	case Previous.String():
		fillText = "FILLPREVIOUS"
	case Next.String():
		fillText = "FILLNEXT"
	case Interpolate.String():
		fillText = "INTERPOLATE"
	}
	return fillText
}

func (protoParser *ProtoParser) getShift(selectStatement SelectStatement, auto string) (string, error) {
	if selectStatement.hasLast {
		if selectStatement.last.isDuration {
			return protoParser.parseShift(selectStatement.last.last) + " " + auto + " / ", nil
		}
		return sampleShiftSpan, nil
	}

	from := selectStatement.from.from.lit
	if selectStatement.from.from.tokenType == STRING {
		from = "'" + selectStatement.from.from.lit + "' TOTIMESTAMP"
	}

	last, err := protoParser.getLastTimestamp(selectStatement)

	if err != nil {
		return "", err
	}
	return last + " " + from + " -", nil
}

// getReducer Generate a Reducer statement
func (protoParser *ProtoParser) getReducer(framework FrameworkStatement, prefix string) string {
	var buffer bytes.Buffer
	operatorString := framework.attributes[Aggregator].lit

	keepDistinct := false
	if attribute, has := framework.attributes[KeepDistinct]; has {
		keepDistinct = attribute.tokenType == TRUE
	}

	reducerParams := ""
	if framework.attributes[Aggregator].tokenType == JOIN || framework.attributes[Aggregator].tokenType == PERCENTILE {
		if framework.unNamedAttributes[0].tokenType == STRING {
			reducerParams = "'" + framework.unNamedAttributes[0].lit + "' "
		} else {
			reducerParams = framework.unNamedAttributes[0].lit + " "
		}
		delete(framework.unNamedAttributes, 0)
	}
	operator := reducerParams + "reducer." + operatorString

	if keepDistinct {
		framework.unNamedAttributes[len(framework.unNamedAttributes)] = InternalField{lit: "hash_945fa9bc3027d7025e3"}
	}
	labelsString := protoParser.getLabelsString(framework.unNamedAttributes)

	if framework.operator == GROUPWITHOUT {
		buffer.WriteString("[ SWAP ")
		buffer.WriteString("DUP  <% DROP LABELS KEYLIST %> LMAP FLATTEN UNIQUE ")
		buffer.WriteString("->SET ")
		buffer.WriteString(labelsString)
		buffer.WriteString(" ->SET DIFFERENCE SET-> ")
		buffer.WriteString(operator + " ] REDUCE ")
		return buffer.String()
	}

	reducer := fmt.Sprintf("[ SWAP " + labelsString + " " + operator + " ] REDUCE ")
	if keepDistinct {
		reducer = "<% DROP DUP { 'hash_945fa9bc3027d7025e3' ROT NAME } RELABEL %> LMAP\n" + prefix + reducer
		reducer += "\n<% DROP { 'hash_945fa9bc3027d7025e3' '' } RELABEL %> LMAP"
	}

	return reducer
}

// getMapper Generate a Mapper statement
func (protoParser *ProtoParser) getMapper(framework FrameworkStatement, sampleSpan string) string {
	// Aggretor must be set otherwise bucket wasn't validated

	if framework.operator == RATE {
		mapper := "[ SWAP mapper.rate 1 1 0 ] MAP "
		if attribute, ok := framework.attributes[MapperValue]; ok {
			value := ""
			if attribute.tokenType == STRING {
				value = "'" + attribute.lit + "'"
			} else if attribute.tokenType == DURATIONVAL {
				value = protoParser.parseShift(attribute.lit)
			} else {
				value = attribute.lit
			}
			value = value + " "
			mapper += "[ SWAP " + value + " 1 s / mapper.mul 0 0 0 ] MAP "
		}
		return mapper
	}

	operator := framework.operator.String()
	mapper := "mapper." + operator

	switch framework.operator {
	case STDDEV, STDVAR, LOG2, LOG10, LOGN, LN, DAY, MAXWITH, MINUTE, MINWITH, HOUR, MONTH, WEEKDAY, YEAR, TIMESTAMP:
		mapper = toWarpScript[framework.operator]
	case EQUAL, GREATEROREQUAL, GREATERTHAN, LESSOREQUAL, LESSTHAN, NOTEQUAL:
		mapper = "mapper." + toWarpScript[framework.operator]
	case CUMULATIVE, WINDOW:
		aggregator := framework.attributes[Aggregator]

		mapper = "mapper." + aggregator.tokenType.String()
		switch aggregator.tokenType {
		case STDDEV, STDVAR:
			mapper = toWarpScript[aggregator.tokenType]
		}
	case DIVSERIES:
		mapper = "mapper.mul"
	}

	value := ""
	if attribute, ok := framework.attributes[MapperValue]; ok {
		if attribute.tokenType == STRING {
			value = "'" + attribute.lit + "'"
		} else if attribute.tokenType == NUMBER && strings.HasPrefix(attribute.lit, ".") {
			value = "0" + attribute.lit
		} else if attribute.tokenType == NEGNUMBER && strings.HasPrefix(attribute.lit, "-.") {
			value = "-0" + strings.Trim(attribute.lit, "-")
		} else {
			value = attribute.lit
			if attribute.tokenType == NUMBER && (framework.operator == ADDSERIES || framework.operator == SUBSERIES || framework.operator == MULSERIES || framework.operator == DIVSERIES) {
				value += " TODOUBLE "
			}

			if framework.operator == DIVSERIES {
				value = "1.0 " + value + " /"
			}
		}
		value = value + " "
	}

	pre := "0"
	post := "0"
	if attribute, hasSampler := framework.attributes[MapperSampling]; hasSampler {
		mapSampler := protoParser.parseShift(attribute.lit)

		pre = mapSampler + " " + sampleSpan + " / ROUND"
	} else {

		if attribute, ok := framework.attributes[MapperPre]; ok {
			if attribute.tokenType == INTEGER {
				pre = attribute.lit
			} else if attribute.tokenType == DURATIONVAL {
				pre = protoParser.parseShift(attribute.lit) + " " + sampleSpan + " /"
			}
		}

		if attribute, ok := framework.attributes[MapperPost]; ok {
			if attribute.tokenType == INTEGER {
				post = attribute.lit
			} else if attribute.tokenType == DURATIONVAL {
				post = protoParser.parseShift(attribute.lit) + " " + sampleSpan + " /"
			}
		}
	}

	occurrences := "0"

	if attribute, hasOccurences := framework.attributes[MapperOccurences]; hasOccurences {
		if attribute.tokenType == INTEGER {
			occurrences = attribute.lit
		}
	}

	return fmt.Sprintf("[ SWAP " + value + mapper + " " + pre + " " + post + " " + occurrences + " ] MAP ")
}

// operators generate WarpScript line for an individual statement
func (protoParser *ProtoParser) operators(framework FrameworkStatement) string {
	operatorString := toWarpScript[framework.operator]

	value := ""

	if attribute, ok := framework.attributes[MapperValue]; ok {
		if attribute.tokenType == STRING {
			value = "'" + attribute.lit + "' "
		} else if attribute.tokenType == DURATIONVAL {
			value = protoParser.parseShift(attribute.lit)
		} else {
			value = attribute.lit
		}
		value = value + " "
	} else if len(framework.unNamedAttributes) > 0 {

		paramStrings := make([]string, len(framework.unNamedAttributes))
		for key, attribute := range framework.unNamedAttributes {
			paramValue := ""
			if attribute.tokenType == STRING {
				paramValue = "'" + attribute.lit + "' "
			} else if attribute.tokenType == DURATIONVAL {
				paramValue = protoParser.parseShift(attribute.lit)
			} else {
				paramValue = attribute.lit
			}
			paramStrings[key] = paramValue
		}

		value = strings.Join(paramStrings, " ")
		value = value + " "
	}

	return value + operatorString
}

// operators generate WarpScript line for an individual statement
func (protoParser *ProtoParser) filterWarpScript(framework FrameworkStatement) (string, error) {
	value := "[ SWAP [] "

	filterType := ""
	attributesSize := len(framework.unNamedAttributes)

	paramStrings := make([]string, len(framework.unNamedAttributes))
	for key, attribute := range framework.unNamedAttributes {

		paramValue := ""
		if attribute.tokenType == STRING {
			paramValue = attribute.lit
		}
		paramStrings[key] = paramValue
	}

	suffix := ""

	switch framework.operator {
	case FILTERBYLASTVALUE:

		filterType = "last."

		if attributesSize != 1 {
			message := FILTERBYLASTVALUE.String() + " expects only one value"
			return "", protoParser.NewProtoError(message, framework.pos)
		}
		if strings.HasPrefix(paramStrings[0], "<=") {
			filterType += "le"
			paramStrings[0] = strings.TrimPrefix(paramStrings[0], "<=")

		} else if strings.HasPrefix(paramStrings[0], "<") {
			filterType += "lt"
			paramStrings[0] = strings.TrimPrefix(paramStrings[0], "<")

		} else if strings.HasPrefix(paramStrings[0], "!=") {
			filterType += "ne"
			paramStrings[0] = strings.TrimPrefix(paramStrings[0], "!=")

		} else if strings.HasPrefix(paramStrings[0], ">=") {
			filterType += "ge"
			paramStrings[0] = strings.TrimPrefix(paramStrings[0], ">=")

		} else if strings.HasPrefix(paramStrings[0], ">") {
			filterType += "gt"
			paramStrings[0] = strings.TrimPrefix(paramStrings[0], ">")

		} else if strings.HasPrefix(paramStrings[0], "=") {
			filterType += "eq"
			paramStrings[0] = strings.TrimPrefix(paramStrings[0], "=")

		} else {
			message := "last value first caracter must be one a lower, geater, equal or not sign"
			return "", protoParser.NewProtoError(message, framework.pos)
		}

	case FILTERBYLABELS:
		filterType = "bylabels"
		for index, label := range paramStrings {
			whereItem, err := protoParser.getWhereField(label, framework.pos)
			if err != nil {
				return "", err
			}
			paramStrings[index] = "'" + whereItem.key + "' '" + whereItem.op.String() + whereItem.value + "'"
		}
		value += "{ "

	case FILTERBYNAME:

		if attributesSize != 1 {
			message := FILTERBYLASTVALUE.String() + " expects only one value"
			return "", protoParser.NewProtoError(message, framework.pos)
		}
		filterType = "byclass"
		paramStrings[0] = "'" + paramStrings[0] + "'"
	}

	value += strings.Join(paramStrings, " ")

	if framework.operator == FILTERBYLABELS {
		value += " }"
	}

	suffix += " ] FILTER"

	return value + " filter." + filterType + suffix, nil
}

// Where field validator
func (protoParser *ProtoParser) getWhereField(lit string, pos Pos) (*WhereField, error) {

	// Load all possible operators
	values := []MatchType{EqualMatch, RegexMatch, NotEqualMatch, RegexNoMatch}

	// Instantiate future where and set a negative max length
	maxLength := -1
	whereField := &WhereField{}

	// Loop over all possibles operators
	for _, value := range values {

		// Check if the lit string is a possible candidat
		if !strings.Contains(lit, value.String()) {
			continue
		}

		// Check if no operator has been found before
		if maxLength != -1 {

			// Otherwise verify that operator index is better than current operator
			if strings.Index(lit, value.String()) > maxLength {
				continue
			}
		}

		// Save current string index of the matching operator
		maxLength = strings.Index(lit, value.String())

		// Prepare Where items by splitting the key
		items := strings.Split(lit, value.String())

		// Set where key
		whereField.key = items[0]

		// Remove where key from items
		items[0] = ""

		// Set where op and value
		whereField.op = value
		whereField.value = strings.Join(items, "")
	}

	// If no operator found send an error to the end user
	if maxLength == -1 {
		errMessage := fmt.Sprintf("Error when parsing field %q in %q function", lit, WHERE.String())
		return nil, protoParser.NewProtoError(errMessage, pos)
	}

	return whereField, nil
}

// nValuesOperators generate WarpScript line for an individual operator containing several parameters
func (protoParser *ProtoParser) nValuesOperators(framework FrameworkStatement) string {
	operatorString := toWarpScript[framework.operator]

	value := ""

	for _, attribute := range framework.unNamedAttributes {
		if attribute.tokenType == STRING {
			value = "'" + attribute.lit + "' "
		} else if attribute.tokenType == DURATIONVAL {
			value = protoParser.parseShift(attribute.lit)
		} else {
			value = attribute.lit
		}
		value = value + " "
	}

	return value + operatorString
}

// operators generate WarpScript line for an individual statement
func (protoParser *ProtoParser) addNamePrefix(framework FrameworkStatement) (string, error) {
	value := ""

	if attribute, ok := framework.unNamedAttributes[0]; ok {
		if attribute.tokenType != STRING {
			message := "to add a prefix name expects a label name as STRING"
			return "", protoParser.NewProtoError(message, framework.pos)
		}

		value = "<% DROP DUP NAME '" + attribute.lit + "' SWAP + RENAME %> LMAP "
	}

	return value, nil
}

// operators generate WarpScript line for an individual statement
func (protoParser *ProtoParser) removeLabels(framework FrameworkStatement) (string, error) {
	value := ""

	if len(framework.unNamedAttributes) == 0 {
		value = "<% DROP DUP LABELS { SWAP <% DROP '' %> FOREACH } RELABEL %> LMAP"
		return value, nil
	}

	value = "<% DROP { "

	for _, attribute := range framework.unNamedAttributes {

		if attribute.tokenType != STRING {
			message := "remove a label expects a labels name as STRING"
			return "", protoParser.NewProtoError(message, framework.pos)
		}

		value = value + "'" + attribute.lit + "' '' "
	}

	value = value + "} RELABEL %> LMAP "

	return value, nil
}

// operators generate WarpScript line for an individual statement
func (protoParser *ProtoParser) renameBy(framework FrameworkStatement) (string, error) {
	value := ""

	if len(framework.unNamedAttributes) > 0 {

		value = "<% DROP DUP LABELS 'labels' STORE '' 'prefix' STORE false 'toRename' STORE '' "

		params := make([]InternalField, len(framework.unNamedAttributes))
		for key, attribute := range framework.unNamedAttributes {
			params[key] = attribute
		}
		for _, attribute := range params {
			value += "<% $labels '" + attribute.lit + "' CONTAINSKEY %> <% $prefix SWAP '" + attribute.lit + "' GET + + '-' 'prefix' STORE true 'toRename' STORE %> <% DROP %> IFTE "
		}

		value += " <% $toRename %> <% RENAME %> <% DROP %> IFTE %> LMAP"
	}

	return value, nil
}

// operators generate WarpScript line for an individual statement
func (protoParser *ProtoParser) renameLabelKey(framework FrameworkStatement) (string, error) {

	old, ok := framework.unNamedAttributes[0]

	if !ok {
		message := "renameLabelKey expects labels name as STRING"
		return "", protoParser.NewProtoError(message, framework.pos)
	}
	new, ok := framework.unNamedAttributes[1]

	if !ok {
		message := "renameLabelKey expects labels name as STRING"
		return "", protoParser.NewProtoError(message, framework.pos)
	}

	if old.tokenType != STRING || new.tokenType != STRING {
		message := "renameLabelKey expects labels name as STRING"
		return "", protoParser.NewProtoError(message, framework.pos)
	}

	value := "<% DROP DUP LABELS '" + old.lit + "' GET { '" + new.lit + "' ROT '" + old.lit + "' '' } RELABEL %> LMAP "

	return value, nil
}

// operators generate WarpScript line for an individual statement
func (protoParser *ProtoParser) renameLabelValue(framework FrameworkStatement) (string, error) {
	labelKey, ok := framework.unNamedAttributes[0]

	if !ok {
		message := "renameLabelValue expects labels name as STRING"
		return "", protoParser.NewProtoError(message, framework.pos)
	}

	regExp, ok := framework.unNamedAttributes[1]

	if !ok {
		message := "renameLabelValue expects labels name as STRING"
		return "", protoParser.NewProtoError(message, framework.pos)
	}

	newValue, ok := framework.unNamedAttributes[2]

	if !ok {
		message := "renameLabelValue expects labels name as STRING"
		return "", protoParser.NewProtoError(message, framework.pos)
	}

	if labelKey.tokenType != STRING || regExp.tokenType != STRING || newValue.tokenType != STRING {
		message := "renameLabelValue expects labels name as STRING"
		return "", protoParser.NewProtoError(message, framework.pos)
	}

	value := "<% DROP DUP LABELS '" + labelKey.lit + "' GET '" + regExp.lit + "' MATCHER MATCH <% SIZE 0 > %> <% { '" + labelKey.lit + "' '" + newValue.lit + "' } RELABEL %> IFT %> LMAP "
	return value, nil
}

func (protoParser *ProtoParser) parseKeepValues(framework FrameworkStatement) (string, error) {
	var buffer bytes.Buffer

	buffer.WriteString("<% DROP ")
	value := "1"
	mapValue, ok := framework.attributes[MapperValue]

	if ok {
		value = mapValue.lit
	}

	buffer.WriteString(value)

	// Keep the minimal value between USER specified parameter and the current SERIES size to avoid a Warp 10 error
	buffer.WriteString(" SWAP DUP SIZE ROT MIN")

	if framework.operator == KEEPLASTVALUES {
		buffer.WriteString(" -1 * ")
	}

	buffer.WriteString(" SHRINK %> LMAP")

	return buffer.String(), nil
}

//
// WarpScript utils methods
//

// getLabelsString generate a list of elements base on an Internal fields map
func (protoParser *ProtoParser) getLabelsString(fields map[int]InternalField) string {
	var buffer bytes.Buffer

	buffer.WriteString("[")

	prefix := " "
	for _, label := range fields {
		buffer.WriteString(prefix)
		newLabels := fmt.Sprintf("%q ", label.lit)
		buffer.WriteString(newLabels)
		prefix = ""
	}
	buffer.WriteString("]")

	return buffer.String()
}

// getLabelsString generate a list of elements base on an Internal fields map
func (protoParser *ProtoParser) getLabelsListString(fields []string) string {
	var buffer bytes.Buffer

	buffer.WriteString("[")

	prefix := " "
	for _, label := range fields {
		buffer.WriteString(prefix)
		newLabels := fmt.Sprintf("%q ", label)
		buffer.WriteString(newLabels)
		prefix = ""
	}
	buffer.WriteString("]")

	return buffer.String()
}

// Get select to forced as Timestamp based on a selectStatement
func (protoParser *ProtoParser) getLastTimestamp(selectStatement SelectStatement) (string, error) {
	if selectStatement.hasFrom {
		if selectStatement.from.hasTo {

			if selectStatement.from.to.tokenType == STRING {
				return "'" + selectStatement.from.to.lit + "' TOTIMESTAMP", nil
			} else if selectStatement.from.to.tokenType == INTEGER {
				return selectStatement.from.to.lit, nil
			}

			message := "dates can only be an INTEGER or STRINGS"
			return "", protoParser.NewProtoError(message, selectStatement.pos)
		}
	} else if selectStatement.hasLast {
		return protoParser.getLastTick(selectStatement)
	}
	return "$now", nil
}

// Get Fetch to value based on a selectStatement
func (protoParser *ProtoParser) getLastTick(selectStatement SelectStatement) (string, error) {
	if selectStatement.hasFrom {
		if selectStatement.from.hasTo {
			if selectStatement.from.to.tokenType == STRING {
				return "'" + selectStatement.from.to.lit + "'", nil
			} else if selectStatement.from.to.tokenType == INTEGER {
				return selectStatement.from.to.lit + " ISO8601", nil
			}

			message := "dates can only be an INTEGER or STRINGS"
			return "", protoParser.NewProtoError(message, selectStatement.pos)
		}
	} else if selectStatement.hasLast {
		last := selectStatement.last

		shift := "0 h"
		if val, ok := last.options[LastShift]; ok {
			shift = protoParser.parseShift(val)
		}

		if val, ok := last.options[LastTimestamp]; ok {
			return val + " " + shift + " -", nil
		}

		if val, ok := last.options[LastDate]; ok {
			return "'" + val + "' TOTIMESTAMP " + shift + " -", nil
		}
	}

	return "$now", nil
}

// Get Fetch from value as string based on a selectStatement
func (protoParser *ProtoParser) getFrom(selectStatement SelectStatement) string {
	if selectStatement.hasFrom {
		if selectStatement.from.from.tokenType == STRING {
			return "'" + selectStatement.from.from.lit + "'"
		}
		return selectStatement.from.from.lit + " ISO8601"
	} else if selectStatement.hasLast {
		last := selectStatement.last

		if last.isDuration {
			return protoParser.parseShift(last.last)
		}

		return "-" + last.last
	}

	return "-1"
}

// Get getFromSampling  return true if from, and from sampling value as number
func (protoParser *ProtoParser) getFromSampling(selectStatement SelectStatement) (bool, string) {
	if selectStatement.hasFrom {
		if selectStatement.from.from.tokenType == STRING {
			return true, "'" + selectStatement.from.from.lit + "' TOTIMESTAMP"
		}
		return true, selectStatement.from.from.lit
	} else if selectStatement.hasLast {
		last := selectStatement.last

		if last.isDuration {
			return false, protoParser.parseShift(last.last)
		}

		return false, "0"
	}

	return false, "0"
}

// Generate shift string value based on a String value
func (protoParser *ProtoParser) parseShift(val string) string {
	twoEnd := val[len(val)-2:]
	end := val[len(val)-1:]

	switch twoEnd {
	case "ms", "us", "ns", "ps":
		return getUnit(val, twoEnd)
	}

	switch end {
	case "w", "d", "h", "m", "s":
		return getUnit(val, end)
	}

	return ""
}

func getUnit(duration string, unit string) string {
	return strings.TrimRight(duration, unit) + " " + unit
}

// Generate labels string from a list of whereField
func (protoParser *ProtoParser) getFetchLabels(labels []WhereField) string {
	if 0 == len(labels) {
		return "{}"
	}

	var buffer bytes.Buffer

	buffer.WriteString("{ ")

	for _, label := range labels {
		labelsValue := label.value
		if label.op == RegexMatch {
			labelsValue = "~" + labelsValue
		} else if label.op == NotEqualMatch || label.op == RegexNoMatch {
			labelsValue = "~(?!" + labelsValue + ").*"
		}

		newLabels := fmt.Sprintf("%q %q ", label.key, labelsValue)
		buffer.WriteString(newLabels)
	}

	buffer.WriteString("}")
	return buffer.String()
}
