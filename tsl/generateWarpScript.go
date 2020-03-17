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
	RENAME:           "RENAME",
	RENAMEBY:         "RENAME",
	SETLABELFROMNAME: "RENAME",
	SHIFT:            "TIMESHIFT",
	SHRINK:           "SHRINK",
	TIMESCALE:        "TIMESCALE",
	TIMECLIP:         "TIMECLIP",
	TIMEMODULO:       "TIMEMODULO FLATTEN",
	TIMESPLIT:        "TIMESPLIT FLATTEN",
	STORE:            "UPDATE",
	RESETS:           "FALSE RESETS",
	DAY:              "'UTC' mapper.day",
	MINUTE:           "'UTC' mapper.minute",
	HOUR:             "'UTC' mapper.hour",
	MONTH:            "'UTC' mapper.month",
	WEEKDAY:          "'UTC' mapper.weekday",
	MAXWITH:          "mapper.max.x",
	MINWITH:          "mapper.min.x",
	YEAR:             "'UTC' mapper.year",
	STDDEV:           "TRUE mapper.sd",
	STDVAR:           "TRUE mapper.var",
	TIMESTAMP:        "mapper.tick",
	LN:               "e mapper.log",
	LOG2:             "2 mapper.log",
	LOG10:            "10 mapper.log",
	LOGN:             "mapper.log",
	BOTTOMNBY:        "SORTBY",
	SORTBY:           "SORTBY",
	SORTDESCBY:       "SORTBY REVERSE",
	TOPNBY:           "SORTBY REVERSE",
	BOTTOMN:          "SORTBY",
	SORT:             "SORTBY",
	SORTDESC:         "SORTBY REVERSE",
	TOPN:             "SORTBY REVERSE",
	EQUAL:            "eq",
	GREATEROREQUAL:   "ge",
	GREATERTHAN:      "gt",
	LESSOREQUAL:      "le",
	LESSTHAN:         "lt",
	NOTEQUAL:         "ne",
	MEAN:             "mean",
}

// GenerateWarpScript Generate Global WarpScript to execute from an instruction list
func (protoParser *ProtoParser) GenerateWarpScript(instructions []Instruction, allowAuthenticate bool) (string, error) {
	var buffer bytes.Buffer

	buffer.WriteString("NOW 'now' STORE\n")
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
	if instruction.isMeta && !(instruction.hasSelect && instruction.selectStatement.isVariable) {
		val, err := protoParser.getMeta(instruction.selectStatement, instruction.connectStatement.token)
		if err != nil {
			return "", err
		}
		return val, nil
	}

	if instruction.hasSelect {

		buffer.WriteString(prefix)
		if len(instruction.createStatement.createSeries) > 0 {
			create, err := protoParser.getCreateSeries(instruction.createStatement, prefix)
			if err != nil {
				return "", nil
			}

			buffer.WriteString(create)

		} else {
			if instruction.selectStatement.isVariable {
				buffer.WriteString("")
			} else {
				fetch, err := protoParser.getFetch(instruction.selectStatement, instruction.connectStatement.token, prefix)
				if err != nil {
					return "", nil
				}

				buffer.WriteString(fetch)
			}
		}
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
			LN, LOG2, LOG10, LOGN, HOUR, MAX, MAXWITH, MEAN, MEDIAN, MIN, MINWITH, MINUTE, MONTH, MULSERIES, NOTEQUAL, ORL, RATE, STDDEV, STDVAR, SUBSERIES,
			ROUND, SQRT, SUM, TIMESTAMP, WEEKDAY, YEAR, JOIN, PERCENTILE, CUMULATIVE, WINDOW, FINITE, TOBOOLEAN, TODOUBLE, TOLONG, TOSTRING:

			buffer.WriteString(protoParser.getMapper(framework, sampleSpan))
			buffer.WriteString("\n")
		case NATIVEVARIABLE:
			pop, err := protoParser.popVariableCall(framework, prefix)
			if err != nil {
				return "", err
			}
			buffer.WriteString(pop)
			buffer.WriteString("\n")
		case QUANTIZE:
			quantize, err := protoParser.quantize(framework, prefix)
			if err != nil {
				return "", err
			}
			buffer.WriteString(quantize)
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

		case RENAMETEMPLATE:
			renameTemplate, err := protoParser.renameTemplate(framework)
			if err != nil {
				return "", err
			}
			buffer.WriteString(renameTemplate)
			buffer.WriteString("\n")

		case FILTERBYLABELS, FILTERBYNAME, FILTERBYLASTVALUE:
			filter, err := protoParser.filterWarpScript(framework)
			if err != nil {
				return "", err
			}
			buffer.WriteString(filter)
			buffer.WriteString("\n")

		case FILTERWITHOUTLABELS:
			filter, err := protoParser.filterWithoutLabelsWarpScript(framework)
			if err != nil {
				return "", err
			}
			buffer.WriteString(filter)
			buffer.WriteString("\n")

		case ADDNAMESUFFIX:
			framework.operator = RENAME

			value := framework.unNamedAttributes[0]
			if value.tokenType == NATIVEVARIABLE {
				value.lit = value.lit + " '%2B' SWAP + "
			} else {
				value.lit = "%2B" + value.lit
			}
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

		case SETLABELFROMNAME:
			rename, err := protoParser.setLabelFromName(framework)
			if err != nil {
				return "", err
			}
			buffer.WriteString(rename)
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

// popVariable generate WarpScript line for a Native Variable statement
func (protoParser *ProtoParser) popVariableCall(framework FrameworkStatement, prefix string) (string, error) {
	var buffer bytes.Buffer

	if len(framework.unNamedAttributes) != 1 {
		errMessage := fmt.Sprintf("Unexpected error in function pop")
		return "", protoParser.NewProtoError(errMessage, framework.pos)
	}

	buffer.WriteString("$" + framework.unNamedAttributes[0].lit + "\n")

	return buffer.String(), nil
}

func (protoParser ProtoParser) getMeta(selectStatement SelectStatement, token string) (string, error) {

	for _, framework := range selectStatement.frameworks {
		switch framework.operator {
		case NAMES, SELECTORS, LABELS, ATTRIBUTES:
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
	case ATTRIBUTES:
		op = "ATTRIBUTES"
	}

	metric := selectStatement.metric

	if selectStatement.metricType == NATIVEVARIABLE {
		metric = "$" + metric
	} else if selectStatement.selectAll {
		metric = "~.*"
	} else {
		metric = "'" + metric + "'"
	}

	suffix := ""

	if (framework.operator == LABELS || framework.operator == ATTRIBUTES) && len(framework.unNamedAttributes) > 0 {
		tagKey := protoParser.getLit(framework.unNamedAttributes[0])
		suffix = tagKey + " GET"
	}

	// Find the series
	find := "[ '" + token + "' " + metric + " " + protoParser.getFetchLabels(selectStatement.where) + " ] FIND"
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

	paramValue := ""
	if len(framework.unNamedAttributes) == 1 {
		paramValue = protoParser.getLit(framework.unNamedAttributes[0])
	}

	bucketizer := paramValue + "bucketizer." + aggregator
	byMacro := ""

	switch attribute.tokenType {
	case NAMES:
		byMacro = "<% NAME %> "
		if framework.operator == TOPN {
			framework.operator = BOTTOMN
		} else if framework.operator == BOTTOMN {
			framework.operator = TOPN
		}
	case SELECTORS:
		byMacro = "<% TOSELECTOR %> "
		if framework.operator == TOPN {
			framework.operator = BOTTOMN
		} else if framework.operator == BOTTOMN {
			framework.operator = TOPN
		}
	case LABELS:
		byMacro = " <% LABELS 'sbLabels' STORE "
		if framework.unNamedAttributes[0].tokenType == INTERNALLIST {
			byMacro += "[ "
			for _, label := range framework.unNamedAttributes[0].fieldList {
				value := protoParser.getLit(label)
				if label.tokenType == IDENT {
					value = protoParser.getStringValue(label.lit)
				}
				byMacro += "$sbLabels " + value + "GET <% DUP ISNULL %> <% DROP '' %> IFT "
			}
			byMacro += " ] '' JOIN "
		} else {
			byMacro += "$sbLabels " + protoParser.getLit(framework.unNamedAttributes[0]) + "GET <% DUP ISNULL %> <% DROP '' %> IFT "
		}
		byMacro += "%> "
		if framework.operator == TOPN {
			framework.operator = BOTTOMN
		} else if framework.operator == BOTTOMN {
			framework.operator = TOPN
		}
	case ATTRIBUTES:
		byMacro = " <% ATTRIBUTES 'sbLabels' STORE "
		if framework.unNamedAttributes[0].tokenType == INTERNALLIST {
			byMacro += "[ "
			for _, label := range framework.unNamedAttributes[0].fieldList {
				value := protoParser.getLit(label)
				if label.tokenType == IDENT {
					value = protoParser.getStringValue(label.lit)
				}
				byMacro += "$sbLabels " + value + "GET <% DUP ISNULL %> <% DROP '' %> IFT "
			}
			byMacro += " ] '' JOIN "
		} else {
			byMacro += "$sbLabels " + protoParser.getLit(framework.unNamedAttributes[0]) + "GET <% DUP ISNULL %> <% DROP '' %> IFT "
		}
		byMacro += "%> "
		if framework.operator == TOPN {
			framework.operator = BOTTOMN
		} else if framework.operator == BOTTOMN {
			framework.operator = TOPN
		}
	default:
		byMacro = "<% [ SWAP " + bucketizer + " 0 0 1 ] BUCKETIZE VALUES 0 GET 0 GET %> "
	}

	operatorString := toWarpScript[framework.operator]

	return byMacro + operatorString + value, nil
}

// Generate a create series statement
func (protoParser *ProtoParser) getCreateSeries(createStatement CreateStatement, prefix string) (string, error) {
	var buffer bytes.Buffer

	buffer.WriteString(prefix + "MAXLONG -1 * 'maxCreateTick' STORE\n")
	buffer.WriteString(prefix + "MAXLONG 'minCreateTick' STORE\n")
	buffer.WriteString(prefix + "[\n")

	for _, createSeries := range createStatement.createSeries {
		buffer.WriteString(prefix + "    NEWGTS ")
		buffer.WriteString(protoParser.getLit(createSeries.metric))
		buffer.WriteString(" RENAME ")
		buffer.WriteString(protoParser.getFetchLabels(createSeries.where))
		buffer.WriteString(" RELABEL")
		buffer.WriteString("\n")

		for _, value := range createSeries.values {
			buffer.WriteString(prefix + "    ")

			tick := protoParser.getLit(*value.tick)

			if createSeries.end.lit != "" {

				end := protoParser.getLit(*createSeries.end)

				if createSeries.end.tokenType == STRING && createSeries.end.lit == NowValue.String() {
					end = "NOW"
				}

				tick = end + " " + tick + " +"
			}

			buffer.WriteString("$maxCreateTick " + tick + " MAX 'maxCreateTick' STORE\n")
			buffer.WriteString("$minCreateTick " + tick + " MIN 'minCreateTick' STORE\n")
			buffer.WriteString(prefix + "    ")
			buffer.WriteString(tick)
			buffer.WriteString(" NaN NaN NaN ")

			valueString := protoParser.getLit(*value.value)

			buffer.WriteString(valueString)
			buffer.WriteString(" ADDVALUE\n")
		}
	}

	buffer.WriteString(prefix + "]\n")
	return buffer.String(), nil
}

// Generate a fetch statement
func (protoParser *ProtoParser) getFetch(selectStatement SelectStatement, token string, prefix string) (string, error) {
	lastTick, err := protoParser.getLastTick(selectStatement)
	if err != nil {
		return "", err
	}
	from := protoParser.getFrom(selectStatement)

	metric := selectStatement.metric
	if selectStatement.metricType == NATIVEVARIABLE {
		metric = "$" + metric
	} else if selectStatement.selectAll {
		metric = "~.*"
	} else {
		metric = "'" + metric + "'"
	}

	// Return find when no last or from methods were sets
	if !selectStatement.hasFrom && !selectStatement.hasLast {
		find := "[ '" + token + "' " + metric + " " + protoParser.getFetchLabels(selectStatement.where) + " ] FIND"
		attPolicy := protoParser.getAttributePolicyString(selectStatement.attributePolicy, prefix)
		return find + attPolicy, nil
	}

	// When has from set return duration between from and lastitck
	if selectStatement.hasFrom {
		var fetch bytes.Buffer
		fetch.WriteString("[ '" + token + "' " + metric + " " + protoParser.getFetchLabels(selectStatement.where) + " " + from + " " + lastTick + " ] FETCH ")
		attPolicy := protoParser.getAttributePolicyString(selectStatement.attributePolicy, prefix)
		return fetch.String() + attPolicy, nil
	}

	// Otherwise return last tick and duration from value in Fetch
	var fetch bytes.Buffer
	fetch.WriteString("[ '" + token + "' " + metric + " " + protoParser.getFetchLabels(selectStatement.where) + " " + lastTick + " " + from + " ] FETCH ")
	attPolicy := protoParser.getAttributePolicyString(selectStatement.attributePolicy, prefix)
	return fetch.String() + prefix + attPolicy, nil
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
			bucketizerParams = protoParser.getLit(framework.unNamedAttributes[0])
		}
	}
	bucketizer := bucketizerParams + "bucketizer." + aggregator

	// Same for span
	var shiftSpan string
	if attribute, ok := framework.attributes[SampleSpan]; ok {
		if attribute.tokenType == DURATIONVAL && strings.HasSuffix(attribute.lit, "M") {
			shiftSpan = attribute.lit
		} else {
			shiftSpan = protoParser.getLit(attribute)
		}
	} else {
		// set default shift span to 0
		shiftSpan = sampleShiftSpan
	}

	hasCount := false
	// Check if current attributes has a count field otherwise use 30 as default value
	if attribute, ok := framework.attributes[SampleAuto]; ok {
		auto = protoParser.getLit(attribute)
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

		relativeSpan := shiftSpan

		if strings.HasSuffix(shiftSpan, "M") {
			relativeSpan = strings.TrimSuffix(shiftSpan, "M") + " 30 d *"
		}

		bucketizePrefix = bucketizePrefix + "$raw " + lasttick + " " + lasttick + " " + lasttick + " " + relativeSpan + " / " + relativeSpan + " * - TIMECLIP NONEMPTY \n" + prefix + "<% SIZE 0 > %>\n"

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

	if strings.HasSuffix(shiftSpan, "M") {
		bucketSpan := strings.TrimSuffix(shiftSpan, "M")
		bucketize := "<% \n"
		bucketize += lastbucket + " TSELEMENTS 1 2 SET 0 3 SET 0 4 SET 0 5 SET 0 6 SET TSELEMENTS-> 1 ADDMONTHS 'endBucketizeMonth' STORE "
		bucketize += `
		$raw
		<%
			DROP
			DUP FIRSTTICK 'firstTickBucketizeMonth' STORE
			$endBucketizeMonth 'tickBucketizeMonth' STORE
			[] 'clipTicks' STORE
			<% $tickBucketizeMonth $firstTickBucketizeMonth > %>
			<% 
			$clipTicks 
			[ 
				$tickBucketizeMonth 
				$tickBucketizeMonth 
				-1 ` + bucketSpan + ` * ADDMONTHS
			]
			+ 'clipTicks' STORE
			$tickBucketizeMonth -1 ` + bucketSpan + ` * ADDMONTHS 'tickBucketizeMonth' STORE
			%>
			WHILE
			$clipTicks
			CLIP FLATTEN NONEMPTY
			[ SWAP bucketizer.last 0 0 1 ] BUCKETIZE 
			<%
				DROP 
				DUP
				CLONEEMPTY
				SWAP
				DUP
				FIRSTTICK 'firstTickBucketized' STORE
				$firstTickBucketized TSELEMENTS
				1 2 SET 0 3 SET 0 4 SET 0 5 SET 0 6 SET TSELEMENTS->
				SWAP
				$firstTickBucketized ATTICK
				4 GET 'valueBucketized' STORE
				NaN NaN NaN $valueBucketized
				ADDVALUE
			%>
			LMAP
			MERGE
		%>
		LMAP
		%> IFT
		`
		return bucketizePrefix + bucketize, shiftSpan, nil
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
		fillValueLit := protoParser.getLit(fillValue)
		fillText = " [ NaN NaN NaN " + fillValueLit + " ] FILLVALUE"
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

	bucketize := "[ $raw " + bucketizer + " " + lastbucket + " " + shiftSpan + " " + auto + " ] BUCKETIZE " + fillText + " UNBUCKETIZE"

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

	from := protoParser.getLit(selectStatement.from.from)
	if selectStatement.from.from.tokenType == STRING {
		from += " TOTIMESTAMP"
	} else if selectStatement.from.from.tokenType == NATIVEVARIABLE {
		from += " DUP TYPEOF <% 'STRING' == %> <% TOTIMESTAMP %> IFT "
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
		reducerParams = protoParser.getLit(framework.unNamedAttributes[0])
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
			value := protoParser.getLit(attribute)
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

		paramValue := ""
		if len(framework.unNamedAttributes) == 1 {
			paramValue = protoParser.getLit(framework.unNamedAttributes[0])
		}
		mapper = paramValue + "mapper." + aggregator.tokenType.String()
		switch aggregator.tokenType {
		case STDDEV, STDVAR:
			mapper = toWarpScript[aggregator.tokenType]
		}
	case DIVSERIES:
		mapper = "mapper.mul"
	case SUBSERIES:
		mapper = "-1 * mapper.add"
	}

	value := ""
	if attribute, ok := framework.attributes[MapperValue]; ok {
		if attribute.tokenType == STRING {
			value = "'" + attribute.lit + "'"
		} else if attribute.tokenType == NUMBER && strings.HasPrefix(attribute.lit, ".") {
			value = "0" + attribute.lit
		} else if attribute.tokenType == NEGNUMBER && strings.HasPrefix(attribute.lit, "-.") {
			value = "-0" + strings.Trim(attribute.lit, "-")
		} else if attribute.tokenType == NATIVEVARIABLE {
			value = "$" + attribute.lit + " "
			if framework.operator == DIVSERIES {
				value = "1.0 " + value + " /"
			}
		} else {
			value = attribute.lit

			if (attribute.tokenType == NUMBER || attribute.tokenType == INTEGER) &&
				(framework.operator == ADDSERIES || framework.operator == SUBSERIES || framework.operator == MULSERIES || framework.operator == DIVSERIES) {
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
		mapSampler := protoParser.getLit(attribute)
		pre = mapSampler + " " + sampleSpan + " / ROUND"
	}

	if attribute, ok := framework.attributes[MapperPre]; ok {
		if attribute.tokenType == INTEGER {
			pre = attribute.lit
		} else if attribute.tokenType == DURATIONVAL {
			pre = "-" + protoParser.parseShift(attribute.lit)
		} else if attribute.tokenType == NATIVEVARIABLE {
			pre = "$" + attribute.lit + " "
		}
	}

	if attribute, ok := framework.attributes[MapperPost]; ok {
		if attribute.tokenType == INTEGER {
			post = attribute.lit
		} else if attribute.tokenType == DURATIONVAL {
			post = "-" + protoParser.parseShift(attribute.lit)
		} else if attribute.tokenType == NATIVEVARIABLE {
			post = "$" + attribute.lit + " "
		}
	}

	occurrences := "0"

	if attribute, hasOccurences := framework.attributes[MapperOccurences]; hasOccurences {
		occurrences = protoParser.getLit(attribute)
	}

	return fmt.Sprintf("[ SWAP " + value + mapper + " " + pre + " " + post + " " + occurrences + " ] MAP ")
}

// quantize generate WarpScript line for a quantize statement
func (protoParser *ProtoParser) quantize(framework FrameworkStatement, prefix string) (string, error) {
	var buffer bytes.Buffer
	hasChunk := false

	for key, attribute := range framework.unNamedAttributes {
		paramValue := protoParser.getLit(attribute)
		if attribute.tokenType == INTERNALLIST {

			paramValue = "[ "

			if len(attribute.fieldList) == 0 {
				errMessage := fmt.Sprintf("Error in function quantize, expects at least a value in step list")
				return "", protoParser.NewProtoError(errMessage, framework.pos)
			}

			for _, internalField := range attribute.fieldList {

				if internalField.tokenType == NATIVEVARIABLE {
					paramValue += "$" + internalField.lit + " "
					continue
				}

				if internalField.tokenType != NUMBER && internalField.tokenType != INTEGER {
					errMessage := fmt.Sprintf("Error in function quantize, expects only integer or number values in step list")
					return "", protoParser.NewProtoError(errMessage, framework.pos)
				}
				paramValue += internalField.lit + " "
			}
			paramValue += "] "
		}
		buffer.WriteString(paramValue)

		if key == 0 {
			buffer.WriteString(prefix + "'labelKey' STORE\n")
		} else if key == 1 {
			buffer.WriteString(prefix + "'step' STORE\n")
		} else if key == 2 {
			buffer.WriteString(prefix + "'duration' STORE\n")
		}
	}
	buffer.WriteString("\n")

	if len(framework.unNamedAttributes) > 2 {
		hasChunk = true
	}

	internalprefix := prefix + ""
	if hasChunk {
		buffer.WriteString(internalprefix + "0 $duration 0 0 '.chunkid' false CHUNK\n")
		buffer.WriteString(internalprefix + "<%\n")
		internalprefix += "    "
		buffer.WriteString(internalprefix + "DROP \n")
	}

	buffer.WriteString(internalprefix + `
	[
    SWAP
    <%
        DUP 'series' STORE
        
        <% $step TYPEOF 'LIST' == %> 
        <% $step %>
        <%
        [ $series mapper.min MAXLONG 0 -1 ] MAP 0 GET VALUES 0 GET 'min' STORE
        [ $series mapper.max MAXLONG 0 -1 ] MAP 0 GET VALUES 0 GET 'max' STORE
        
        $min $step / ROUND $step *  'incrementalStep' STORE
        [ 
            $incrementalStep
            $incrementalStep $step + DUP 'incrementalStep' STORE
            <% $incrementalStep $max < %>
            <%
                $incrementalStep $step + 'incrementalStep' STORE
                $incrementalStep
            %>
            WHILE
        ]
        %>
        IFTE
        DUP 
        DUP SIZE 'length' STORE
        DUP $length 1 - GET 'last' STORE
        <%
            SWAP 'current' STORE
            <% 1 >= %>
            <%
                $previous TOSTRING '<' + 'v<=' + $current TOSTRING +
            %>
            <%
                '<=' $current TOSTRING +
            %>
            IFTE
            $current 'previous' STORE
        %>
        LMAP
        '>' $last TOSTRING + +
        QUANTIZE
        VALUEHISTOGRAM
        [
        SWAP
        <%
            'value' STORE
            'labelValue' STORE
			$series CLONEEMPTY 
	`)
	if hasChunk {
		buffer.WriteString(`
			DUP LABELS '.chunkid' GET TOLONG 
			`)
	} else {
		buffer.WriteString(`
			$series LASTTICK
			`)
	}

	buffer.WriteString(internalprefix + `
            NaN NaN NaN
            $value
            ADDVALUE
            {
                '.chunkid' ''
                $labelKey $labelValue
            }
            RELABEL
        %>
        FOREACH
        ]
     %>
     FOREACH
     ]
     FLATTEN
	[ SWAP [ $labelKey ] reducer.sum ] REDUCE FLATTEN
	`)

	if hasChunk {
		buffer.WriteString(`
		%>
		LMAP
		FLATTEN
		`)
	}
	return buffer.String(), nil
}

// operators generate WarpScript line for an individual statement
func (protoParser *ProtoParser) operators(framework FrameworkStatement) string {
	operatorString := toWarpScript[framework.operator]

	value := ""

	if attribute, ok := framework.attributes[MapperValue]; ok {
		value = protoParser.getLit(attribute)
	} else if len(framework.unNamedAttributes) > 0 {

		paramStrings := make([]string, len(framework.unNamedAttributes))
		for key, attribute := range framework.unNamedAttributes {
			paramValue := protoParser.getLit(attribute)
			paramStrings[key] = paramValue
		}

		value = strings.Join(paramStrings, " ")
		value = value + " "
	}

	if framework.operator == SHRINK {
		return "<% DROP " + value + operatorString + " %> LMAP"

	}

	return value + operatorString
}

func (protoParser *ProtoParser) filterWithoutLabelsWarpScript(framework FrameworkStatement) (string, error) {

	var buffer bytes.Buffer

	buffer.WriteString(`
	<% 
	DUP
	
	0 GET
	
	SWAP FILTER
  
	->SET
  
	SWAP ->SET
  
	SWAP 2 DUPN DIFFERENCE
  
	SET-> SWAP SET->
  
	ROT DROP
  %>
  'neg-filter' CSTORE
`)

	for _, labelKey := range framework.unNamedAttributes {
		if labelKey.tokenType == NATIVEVARIABLE {
			buffer.WriteString(fmt.Sprintf("[ SWAP [] { $%s '~.*' } filter.bylabels ] @neg-filter\n", labelKey.lit))
		} else {
			buffer.WriteString(fmt.Sprintf("[ SWAP [] { '%s' '~.*' } filter.bylabels ] @neg-filter\n", labelKey.lit))
		}
		buffer.WriteString(" DROP \n")
	}

	return buffer.String(), nil
}

// operators generate WarpScript line for an individual statement
func (protoParser *ProtoParser) filterWarpScript(framework FrameworkStatement) (string, error) {
	value := "[ SWAP [] "

	filterType := ""
	attributesSize := len(framework.unNamedAttributes)

	paramStrings := make([]InternalField, len(framework.unNamedAttributes))
	for key, attribute := range framework.unNamedAttributes {
		paramStrings[key] = attribute
	}

	suffix := ""

	switch framework.operator {
	case FILTERBYLASTVALUE:

		filterType = "last."

		if attributesSize != 1 {
			message := FILTERBYLASTVALUE.String() + " expects only one value"
			return "", protoParser.NewProtoError(message, framework.pos)
		}
		if strings.HasPrefix(paramStrings[0].lit, "<=") {
			filterType += "le"
			paramStrings[0].lit = strings.TrimPrefix(paramStrings[0].lit, "<=")

		} else if strings.HasPrefix(paramStrings[0].lit, "<") {
			filterType += "lt"
			paramStrings[0].lit = strings.TrimPrefix(paramStrings[0].lit, "<")

		} else if strings.HasPrefix(paramStrings[0].lit, "!=") {
			filterType += "ne"
			paramStrings[0].lit = strings.TrimPrefix(paramStrings[0].lit, "!=")

		} else if strings.HasPrefix(paramStrings[0].lit, ">=") {
			filterType += "ge"
			paramStrings[0].lit = strings.TrimPrefix(paramStrings[0].lit, ">=")

		} else if strings.HasPrefix(paramStrings[0].lit, ">") {
			filterType += "gt"
			paramStrings[0].lit = strings.TrimPrefix(paramStrings[0].lit, ">")

		} else if strings.HasPrefix(paramStrings[0].lit, "=") {
			filterType += "eq"
			paramStrings[0].lit = strings.TrimPrefix(paramStrings[0].lit, "=")

		} else if paramStrings[0].tokenType == NATIVEVARIABLE {
			filterType = ""
			paramStrings[0].lit = `
			'filter.last.' 'prefixFilterOperator' STORE
			$test
			<% DUP 0 2 SUBSTRING ">=" == %>
			<% 
				2 SUBSTRING EVAL
				$prefixFilterOperator 'ge' + EVAL
			%>
			<% DUP 0 2 SUBSTRING "<=" == %>
			<% 
				2 SUBSTRING EVAL
				$prefixFilterOperator 'le' + EVAL
			%>
			<% DUP 0 2 SUBSTRING "!=" == %>
			<% 
				2 SUBSTRING EVAL
				$prefixFilterOperator 'ne' + EVAL
			%>
			<% DUP 0 1 SUBSTRING "=" == %>
			<% 
				1 SUBSTRING EVAL
				$prefixFilterOperator 'eq' + EVAL
			%>
			<% DUP 0 1 SUBSTRING "<" == %>
			<% 
				1 SUBSTRING EVAL
				$prefixFilterOperator 'lt' + EVAL
			%>
			<% DUP 0 1 SUBSTRING ">" == %>
			<% 
				1 SUBSTRING EVAL
				$prefixFilterOperator 'gt' + EVAL
			%>
			<% 'Unkown operator in filterbyvalue function' MSGFAIL  %>
			6
			SWITCH
			`

		} else {
			message := "last value first caracter must be one a lower, geater, equal or not sign"
			return "", protoParser.NewProtoError(message, framework.pos)
		}

	case FILTERBYLABELS:
		filterType = "bylabels"

		filtersField := make([]WhereField, len(paramStrings))
		for index, label := range paramStrings {
			whereItem, err := protoParser.getWhereField(label.lit, framework.pos, label.tokenType, framework.operator)
			if err != nil {
				return "", err
			}
			filtersField[index] = *whereItem
		}
		value += protoParser.getFetchLabels(filtersField)

	case FILTERBYNAME:

		if attributesSize != 1 {
			message := FILTERBYLASTVALUE.String() + " expects only one value"
			return "", protoParser.NewProtoError(message, framework.pos)
		}
		filterType = "byclass"

		if paramStrings[0].tokenType == NATIVEVARIABLE {
			paramStrings[0].lit = "$" + paramStrings[0].lit
		} else {
			whereItem, err := protoParser.getWhereField(paramStrings[0].lit, framework.pos, paramStrings[0].tokenType, framework.operator)
			if err != nil {
				return "", err
			}
			paramStrings[0].lit = "'" + protoParser.getWhereValueString(*whereItem) + "'"
		}
	}

	paramStringsItem := make([]string, len(paramStrings))

	for key, attribute := range paramStrings {
		paramStringsItem[key] = attribute.lit
	}

	if framework.operator != FILTERBYLABELS {
		value += strings.Join(paramStringsItem, " ")
	}

	suffix += " ] FILTER"

	if paramStrings[0].tokenType == NATIVEVARIABLE {
		return value + suffix, nil
	}
	return value + " filter." + filterType + suffix, nil
}

// Where field validator
func (protoParser *ProtoParser) getWhereField(lit string, pos Pos, token Token, function Token) (*WhereField, error) {

	// Load all possible operators
	values := []MatchType{EqualMatch, RegexMatch, NotEqualMatch, RegexNoMatch}

	// Instantiate future where and set a negative max length
	maxLength := -1
	whereField := &WhereField{whereType: token}

	if token == NATIVEVARIABLE {
		whereField.key = lit
		return whereField, nil
	}

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
		errMessage := fmt.Sprintf("Error when parsing field %q in %q function", lit, function.String())
		return nil, protoParser.NewProtoError(errMessage, pos)
	}

	return whereField, nil
}

// nValuesOperators generate WarpScript line for an individual operator containing several parameters
func (protoParser *ProtoParser) nValuesOperators(framework FrameworkStatement) string {
	operatorString := toWarpScript[framework.operator]

	value := ""

	for _, attribute := range framework.unNamedAttributes {
		value += " " + protoParser.getLit(attribute)
	}

	return value + operatorString
}

// operators generate WarpScript line for an individual statement
func (protoParser *ProtoParser) addNamePrefix(framework FrameworkStatement) (string, error) {
	value := ""

	if attribute, ok := framework.unNamedAttributes[0]; ok {

		attributeLit := protoParser.getLit(framework.unNamedAttributes[0])
		if attribute.tokenType != STRING && attribute.tokenType != NATIVEVARIABLE {
			message := "to add a prefix name expects a label name as STRING"
			return "", protoParser.NewProtoError(message, framework.pos)
		}

		value = "<% DROP DUP NAME " + attributeLit + " SWAP + RENAME %> LMAP "
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

		if attribute.tokenType != STRING && attribute.tokenType != NATIVEVARIABLE {
			message := "remove a label expects a labels name as STRING"
			return "", protoParser.NewProtoError(message, framework.pos)
		}

		attributeLit := protoParser.getLit(attribute)
		value = value + attributeLit + " '' "
	}

	value = value + "} RELABEL %> LMAP "

	return value, nil
}

// operators generate WarpScript line for an individual statement
func (protoParser *ProtoParser) renameTemplate(framework FrameworkStatement) (string, error) {
	value := ""

	if attribute, ok := framework.unNamedAttributes[0]; ok {
		if attribute.tokenType != STRING && attribute.tokenType != NATIVEVARIABLE {
			message := "Rename template expects its parameter to be a STRING"
			return "", protoParser.NewProtoError(message, framework.pos)
		}
		attributeLit := protoParser.getLit(attribute)
		value = "<% DROP DUP 'series' STORE [ " + attributeLit + " ]"
		if strings.Contains(value, "${this.name}") {
			value = strings.Replace(value, "${this.name}", "' $series NAME '", -1)
		}

		labels := strings.Split(value, "${this.labels.")

		if len(labels) > 1 {

			for i, label := range labels {
				if i == 0 {
					continue
				}
				labelKeys := strings.Split(label, "}")

				if len(labelKeys) < 2 {
					message := fmt.Sprintf("expect a } to end current label template: %s}", label)
					return "", protoParser.NewProtoError(message, framework.pos)
				}

				value = strings.Replace(value, "${this.labels."+labelKeys[0]+"}", "' $series LABELS '"+labelKeys[0]+"' GET '", 1)
			}
		}

		value += " '' JOIN RENAME %> LMAP"

	}

	return value, nil
}

// operators generate WarpScript line for an individual statement
func (protoParser *ProtoParser) setLabelFromName(framework FrameworkStatement) (string, error) {
	value := ""

	params := make([]InternalField, len(framework.unNamedAttributes))
	for key, attribute := range framework.unNamedAttributes {
		params[key] = attribute
	}

	label := protoParser.getLit(params[0])
	match := ""
	if len(params) == 2 {
		regex := protoParser.getLit(params[1])
		match = regex + " MATCH DUP SIZE 0 > <% '' 0 SET %> IFT '' JOIN "
	}

	value = "<% DROP DUP { " + label + " ROT NAME " + match + "} RELABEL %> LMAP"

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
			attributeLit := protoParser.getLit(attribute)
			value += "<% $labels " + attributeLit + " CONTAINSKEY %> <% $prefix SWAP " + attributeLit + " GET + + '-' 'prefix' STORE true 'toRename' STORE %> <% DROP %> IFTE "
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

	if (old.tokenType != STRING && old.tokenType != NATIVEVARIABLE) || (new.tokenType != STRING && new.tokenType != NATIVEVARIABLE) {
		message := "renameLabelKey expects labels name as STRING"
		return "", protoParser.NewProtoError(message, framework.pos)
	}

	oldLit := protoParser.getLit(old)
	newLit := protoParser.getLit(new)

	value := "<% DROP DUP LABELS " + oldLit + " GET { " + newLit + " ROT " + oldLit + " '' } RELABEL %> LMAP "

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

	if (labelKey.tokenType != STRING && labelKey.tokenType != NATIVEVARIABLE) ||
		(regExp.tokenType != STRING && regExp.tokenType != NATIVEVARIABLE) ||
		(newValue.tokenType != STRING && newValue.tokenType != NATIVEVARIABLE) {
		message := "renameLabelValue expects labels name as STRING"
		return "", protoParser.NewProtoError(message, framework.pos)
	}

	labelKeyLit := protoParser.getLit(labelKey)
	regExpLit := protoParser.getLit(regExp)
	newValueLit := protoParser.getLit(newValue)
	value := "<% DROP DUP LABELS " + labelKeyLit + " GET " + regExpLit + " MATCHER MATCH <% SIZE 0 > %> <% { " + labelKeyLit + " " + newValueLit + " } RELABEL %> IFT %> LMAP "
	return value, nil
}

func (protoParser *ProtoParser) parseKeepValues(framework FrameworkStatement) (string, error) {
	var buffer bytes.Buffer

	buffer.WriteString("<% DROP ")
	value := "1"
	mapValue, ok := framework.attributes[MapperValue]

	if ok {
		value = protoParser.getLit(mapValue)
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

	if len(fields) == 1 {
		field := fields[0]
		if field.tokenType == NATIVEVARIABLE {
			return "$" + field.lit
		}
	}

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

			value := protoParser.getLit(selectStatement.from.to)
			if selectStatement.from.to.tokenType == NATIVEVARIABLE {
				value += "DUP TYPEOF <% 'STRING' == %> <% TOTIMESTAMP %> IFT "
				return value, nil
			} else if selectStatement.from.to.tokenType == IDENT {
				return value, nil
			} else if selectStatement.from.to.tokenType == STRING {
				return value + " TOTIMESTAMP", nil
			} else if selectStatement.from.to.tokenType == INTEGER {
				return value, nil
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
			value := protoParser.getLit(selectStatement.from.to)
			if selectStatement.from.to.tokenType == NATIVEVARIABLE {
				return value + " DUP TYPEOF <% 'STRING' != %> <% ISO8601 %> IFT ", nil
			} else if selectStatement.from.to.tokenType == STRING {
				return value, nil
			} else if selectStatement.from.to.tokenType == INTEGER {
				return value + " ISO8601", nil
			}

			message := "dates can only be an INTEGER or STRINGS"
			return "", protoParser.NewProtoError(message, selectStatement.pos)
		}
		return "$now ISO8601", nil
	} else if selectStatement.hasLast {
		last := selectStatement.last

		shift := "0 h"

		if val, ok := last.options[LastShift]; ok {
			shift = protoParser.getLit(val)
		}

		if val, ok := last.options[LastTimestamp]; ok {
			lt := protoParser.getLit(val)
			return lt + " " + shift + " -", nil
		}

		if val, ok := last.options[LastDate]; ok {
			ld := protoParser.getLit(val)
			return ld + " TOTIMESTAMP " + shift + " -", nil
		}

		if val, ok := last.options[Unknown]; ok {
			ld := protoParser.getLit(val)
			return ld + " DUP TYPEOF <% 'STRING' == %> <% TOTIMESTAMP %> IFT " + shift + " -", nil
		}

		if shift != "0 h" {
			return "$now " + shift + " -", nil
		}

	}

	return "$now", nil
}

// Get Fetch from value as string based on a selectStatement
func (protoParser *ProtoParser) getFrom(selectStatement SelectStatement) string {
	if selectStatement.hasFrom {
		value := protoParser.getLit(selectStatement.from.from)
		if selectStatement.from.from.tokenType == NATIVEVARIABLE {
			return value + " DUP TYPEOF <% 'STRING' != %> <% ISO8601 %> IFT "
		} else if selectStatement.from.from.tokenType == STRING {
			return value
		}
		return value + " ISO8601"
	} else if selectStatement.hasLast {
		last := selectStatement.last

		if last.lastType == NATIVEVARIABLE {
			return "$" + last.last
		}
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
		value := protoParser.getLit(selectStatement.from.from)
		if selectStatement.from.from.tokenType == STRING {
			return true, value + " TOTIMESTAMP"
		}
		return true, value
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
	if len(labels) == 0 {
		return "{}"
	}

	var buffer bytes.Buffer

	buffer.WriteString("{ ")

	for _, label := range labels {
		if label.whereType == NATIVEVARIABLE {
			buffer.WriteString("$" + label.key)
			whereVar := `
			DUP TYPEOF 
			<%
				'LIST' !=
			%> 
			<%
				1 ->LIST
			%>
			IFT 
			<%
				DUP '=' SPLIT
				<% 
					DUP SIZE 2 == 
				%>
				<% 
					SWAP DROP LIST-> DROP
					CONTINUE
				%>
				IFT
				
				DROP
				'~' SPLIT
				<% 
					DUP SIZE 2 == 
				%>
				<% 
					LIST-> DROP
					'~' SWAP +
					CONTINUE
				%>
				IFT
				
				'Labels fields expects a "=" or a "~" as key value separator' MSGFAIL
			%>
			FOREACH
			`
			buffer.WriteString(whereVar)
		} else {
			labelsValue := protoParser.getWhereValueString(label)
			newLabels := protoParser.getStringValue(label.key) + " " + labelsValue
			buffer.WriteString(newLabels)
		}
	}

	buffer.WriteString("}")
	return buffer.String()
}

func (protoParser *ProtoParser) getWhereValueString(label WhereField) string {
	labelsValue := label.value
	if label.op == RegexMatch {
		labelsValue = "~" + labelsValue
	} else if label.op == NotEqualMatch || label.op == RegexNoMatch {
		labelsValue = "~(?!" + labelsValue + ").*"
	}

	return protoParser.getStringValue(labelsValue)
}

func (protoParser *ProtoParser) getLit(field InternalField) string {

	switch field.tokenType {
	case NATIVEVARIABLE:
		return "$" + field.lit + " "
	case STRING:
		return protoParser.getStringValue(field.lit)
	case DURATIONVAL:
		return protoParser.parseShift(field.lit) + " "
	case NOW:
		return "$now"
	default:
		return field.lit + " "
	}
}

func (protoParser *ProtoParser) getStringValue(lit string) string {
	value := "'" + lit + "' "
	if strings.Contains(lit, "${this.nativevariable.") {
		variables := strings.Split(lit, "${this.nativevariable.")
		if len(variables) > 1 {

			prefix := ""
			for i, variable := range variables {
				if i == 0 {
					continue
				}
				variableKeys := strings.Split(variable, "}")

				value = strings.Replace(value, "${this.nativevariable."+variableKeys[0]+"}", "' "+prefix+"$"+variableKeys[0]+" TOSTRING + '", 1)
				prefix = "+ "
			}

			value += "+ "
		}
	}
	return value
}
