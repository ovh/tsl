package tsl

import (
	"encoding/base64"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

var reZeroOnly = regexp.MustCompile("^\\s*0+\\s*$")

//
// Define TSL parser utils methods
//

// Parser represents a TSL parser
type Parser struct {
	s             *bufScanner
	params        map[string]interface{}
	variables     map[string]*Variable
	lineStart     int
	defaultURI    string
	defaultToken  string
	samplersCount string
	hasQueryRange bool
	queryRange    *QueryRange
}

//QueryRange struct when user set query-range header
type QueryRange struct {
	defaultIsLast       bool
	defaultLastDuration string
	defaultFromStart    InternalField
	defaultFromTo       InternalField
}

// NewParser returns a new instance of Parser
func NewParser(r io.Reader, defaultURI, defaultToken string, lineHeader int, queryRange string, samplersCount string) (*Parser, error) {

	hasQueryRange := false
	parserQueryRange := &QueryRange{}
	if queryRange != "" {
		err := parserQueryRange.queryRangeParser(queryRange)
		if err != nil {
			return nil, err
		}
		hasQueryRange = true
	}

	lit := "100"
	if samplersCount != "" {
		var tok Token
		scanner := newBufScanner(strings.NewReader(string(samplersCount)))
		tok, _, lit = scanner.Scan()
		if tok != INTEGER {
			return nil, fmt.Errorf("Error in header %q, expects an Integer number", samplersCountHeader)
		}
	}
	return &Parser{s: newBufScanner(r), defaultURI: defaultURI, defaultToken: defaultToken, lineStart: lineHeader,
		hasQueryRange: hasQueryRange, queryRange: parserQueryRange, samplersCount: lit}, nil
}

func (qr *QueryRange) queryRangeParser(queryRange string) error {

	items := make([]string, 1)
	items[0] = queryRange
	if strings.Contains(queryRange, ",") {
		items = strings.Split(queryRange, ",")
	}

	for index, timeItem := range items {

		scanner := newBufScanner(strings.NewReader(string(timeItem)))

		tok, _, lit := scanner.Scan()
		if index == 0 && tok == DURATIONVAL {
			// Set last default value
			qr.defaultIsLast = true
			qr.defaultLastDuration = lit
			continue
		}

		if (tok == INTEGER || tok == NUMBER) && lit == timeItem {
			if index == 0 {
				qr.defaultFromStart = InternalField{tokenType: tok, prefixName: FromFrom, hasPrefixName: true, lit: timeItem}
			} else if index == 1 {
				qr.defaultFromTo = InternalField{tokenType: tok, prefixName: FromTo, hasPrefixName: true, lit: timeItem}
			}
		} else {
			// Get a from value
			_, err := time.Parse(time.RFC3339, timeItem)

			if err != nil {
				return fmt.Errorf("Error in header %q, expects a valid RFC3339 date", queryRandeHeader)
			}

			if index == 0 {
				qr.defaultFromStart = InternalField{tokenType: STRING, prefixName: FromFrom, hasPrefixName: true, lit: timeItem}
			} else if index == 1 {
				qr.defaultFromTo = InternalField{tokenType: STRING, prefixName: FromTo, hasPrefixName: true, lit: timeItem}
			}
		}
	}
	return nil
}

// Scan returns the next token from the underlying scanner
func (p *Parser) Scan() (tok Token, pos Pos, lit string) { return p.s.Scan() }

// ScanIgnoreWhitespace scans the next non-whitespace and non-comment token
func (p *Parser) ScanIgnoreWhitespace() (tok Token, pos Pos, lit string) {
	for {
		tok, pos, lit = p.Scan()
		if tok == WS || tok == COMMENT {
			continue
		}
		return
	}
}

// ScanIgnoreDOT scans the next DOT token
func (p *Parser) ScanIgnoreDOT() (tok Token, pos Pos, lit string) {
	for {
		tok, pos, lit = p.Scan()
		if tok == DOT {
			continue
		}
		return
	}
}

// Unscan pushes the previously token back onto the buffer.
func (s *bufScanner) Unscan() { s.n++ }

// Unscan pushes the previously read token back onto the buffer.
func (p *Parser) Unscan() { p.s.Unscan() }

//
// TSL instructions parser
//

// Parse a TSL string into a generic query
func (p *Parser) Parse() (*Query, error) {
	var statements Statements
	connectStatement := &ConnectStatement{api: p.defaultURI, token: p.defaultToken, pos: Pos{Line: 0, Char: 0}}
	p.variables = make(map[string]*Variable)

	// For each new elements split per a space, line or comment to start parsing each single instruction
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok == EOF {
			return &Query{Statements: statements}, nil
		}
		p.Unscan()
		s, newConnectStatement, err := p.ParseStatement(connectStatement, false, false)
		connectStatement = newConnectStatement
		if err != nil {
			return nil, err
		}
		if s.hasSelect || s.isGlobalOperator {
			statements = append(statements, s)
		}
	}
}

// ParseStatement parses one and only one instruction
func (p *Parser) ParseStatement(oldConnectStatement *ConnectStatement, internCall bool, loadVariable bool) (*Instruction, *ConnectStatement, error) {

	// Start instruction
	instruction := &Instruction{}
	instruction.createStatement.createSeries = make([]CreateSeries, 0)
	instruction.connectStatement = *oldConnectStatement
	newConnectStatement := oldConnectStatement
	var err error

	// For each methods split per DOT
loop:
	for {
		tok, pos, lit := p.ScanIgnoreDOT()

		// Parse first instruction methods, that can be or CONNECT or SELECT
		switch tok {

		case CREATE:

			// Parse select intern attributes
			instruction, err = p.parseCreate(tok, pos, lit, instruction)

			if err != nil {
				return nil, nil, err
			}

			// Set has select instruction
			instruction.hasSelect = true

			// Parse post select methods
			instruction, err = p.parseTimesSeriesOperators(instruction, internCall)

			if err != nil {
				return nil, nil, err
			}
			break loop

		case SELECT:

			// Parse select intern attributes
			instruction, err = p.parseSelect(tok, pos, lit, instruction)

			if err != nil {
				return nil, nil, err
			}

			// Set has select instruction
			instruction.hasSelect = true

			// Parse post select methods
			instruction, err = p.parsePostSelectStatement(instruction, internCall)

			if err != nil {
				return nil, nil, err
			}
			break loop
		case CONNECT:

			if internCall {
				return nil, nil, fmt.Errorf("Function %q isn't allowed in an operator at %d, char %d", CONNECT.String(), pos.Line+1, pos.Char+1)
			}

			if loadVariable {
				return nil, nil, fmt.Errorf("Function %q isn't allowed when declaring a variable at %d, char %d", CONNECT.String(), pos.Line+1, pos.Char+1)
			}
			// Parse connect attributes
			instruction, err = p.parseConnect(tok, pos, lit, instruction)

			// Set future instruction with current connect query
			newConnectStatement = &instruction.connectStatement
			if err != nil {
				return nil, nil, err
			}

		case ADDSERIES, ANDL, DIVSERIES, EQUAL, GREATEROREQUAL, GREATERTHAN, LESSOREQUAL,
			LESSTHAN, MULSERIES, NOTEQUAL, ORL, SUBSERIES:
			instruction, err = p.parseGlobalSeriesOp(tok, pos, lit, instruction, -1, loadVariable)

			if err != nil {
				return nil, nil, err
			}

			// Parse post operators methods
			instruction, err = p.parsePostOperatorStatement(instruction, internCall)

			if err != nil {
				return nil, nil, err
			}
			break loop

		case MASK, NEGMASK:
			instruction, err = p.parseGlobalSeriesOp(tok, pos, lit, instruction, 2, loadVariable)

			if err != nil {
				return nil, nil, err
			}

			// Parse post operators methods
			instruction, err = p.parsePostOperatorStatement(instruction, internCall)

			if err != nil {
				return nil, nil, err
			}
			break loop

		// When an ident is found might corresponds to a new variable declaration
		case IDENT:

			nexTok, _, _ := p.ScanIgnoreWhitespace()

			if nexTok != EQ {

				p.Unscan()

				instruction, err = p.parsePostVariables(pos, lit, instruction.connectStatement, false)
				if err != nil {
					return nil, nil, err
				}
				break loop
			}

			if internCall {
				return nil, nil, fmt.Errorf("Cannot declared a variable inside an operator at line %d, char %d", pos.Line+1, pos.Char+1)
			}

			if loadVariable {
				return nil, nil, fmt.Errorf("A variable cannot be declared inside a variable at line %d, char %d", pos.Line+1, pos.Char+1)
			}

			nexTok, nextPos, nextLit := p.ScanIgnoreWhitespace()
			p.variables[lit], err = p.parseVariableDec(nexTok, nextPos, nextLit, lit)

			if err != nil {
				return nil, nil, err
			}
			break loop

		// Stay in instruction as long as the next word start with a DOT (ignore commentary)
		case WS, COMMENT:
			nexTok, _, _ := p.ScanIgnoreWhitespace()
			if nexTok != DOT {
				p.Unscan()
				break loop
			}

		// Stop at end of file
		case EOF:
			break loop

		// Stop at comma or RPAREN when Internal call
		case COMMA, RPAREN:
			if internCall {
				p.Unscan()
				break loop
			}

		// Send an error for all other case
		default:
			log.Debug(tok, pos, lit)
			return nil, nil, fmt.Errorf("Unexpected reserved keyword to start instruction at line %d, char %d", pos.Line+1, pos.Char+1)
		}
	}
	return instruction, newConnectStatement, nil
}

func (p *Parser) parsePostVariables(pos Pos, lit string, connectStatement ConnectStatement, internCall bool) (*Instruction, error) {
	internalInstruction := &Instruction{}
	variable, exists := p.variables[lit]
	var err error

	if !exists {
		errMessage := fmt.Sprintf("Variable %q doesn't exists", lit)
		return nil, p.NewTslError(errMessage, pos)
	}

	*internalInstruction = variable.instruction

	internalInstruction = setConnectCall(internalInstruction, connectStatement)

	switch variable.tokenType {
	case SELECT:

		// Parse post select methods
		nexTok, _, _ := p.ScanIgnoreWhitespace()
		p.Unscan()

		if nexTok == DOT {
			internalInstruction, err = p.parsePostSelectStatement(internalInstruction, internCall)
		}
		if err != nil {
			return nil, err
		}

	case GTSLIST:
		// Parse post GTSLIST methods

		nexTok, _, _ := p.ScanIgnoreWhitespace()
		p.Unscan()

		if nexTok == DOT {
			internalInstruction, err = p.parseTimesSeriesOperators(internalInstruction, internCall)
		}

		if err != nil {
			return nil, err
		}

	case MULTIPLESERIESOPERATOR:

		// Parse post operators methods
		nexTok, _, _ := p.ScanIgnoreWhitespace()
		p.Unscan()

		if nexTok == DOT {
			internalInstruction, err = p.parsePostOperatorStatement(internalInstruction, internCall)
		}

		if err != nil {
			return nil, err
		}
	}

	return internalInstruction, nil
}

func setConnectCall(instruction *Instruction, connectStatement ConnectStatement) *Instruction {
	instruction.connectStatement = connectStatement

	if instruction.isGlobalOperator {
		for i, internalInstruction := range instruction.globalOperator.instructions {
			if internalInstruction.isGlobalOperator {
				internalInstruction.globalOperator.instructions[i] = setConnectCall(internalInstruction, connectStatement)
			}
			internalInstruction.connectStatement = connectStatement
			instruction.globalOperator.instructions[i] = internalInstruction
		}
	}

	return instruction
}

// Connect TSL method parser
func (p *Parser) parseVariableDec(tok Token, pos Pos, lit string, name string) (*Variable, error) {

	variable := &Variable{}
	variable.fieldList = make([]InternalField, 0)

	switch tok {

	// Case list of items
	case LBRACKET:
		variable.tokenType = INTERNALLIST
		variable.lit = lit
		variable.name = name
		field, err := p.ParseInternalFieldList("Variable "+name, InternalField{tokenType: INTERNALLIST})
		if err != nil {
			return nil, err
		}
		variable.fieldList = field.fieldList
	// Case basic varables
	case STRING, INTEGER, NUMBER, DURATIONVAL, TRUE, FALSE, NEGINTEGER, NEGNUMBER:
		variable.tokenType = tok
		variable.lit = lit
		variable.name = name
	default:
		// Parse select intern attributes
		p.Unscan()
		internalInstruction, _, err := p.ParseStatement(&ConnectStatement{}, true, true)

		if err != nil {
			return nil, err
		}

		if internalInstruction.hasSelect {

			if len(internalInstruction.selectStatement.frameworks) > 0 {
				variable.tokenType = GTSLIST
			} else {
				variable.tokenType = SELECT
			}

		} else if internalInstruction.isGlobalOperator {
			variable.tokenType = MULTIPLESERIESOPERATOR
		} else {
			errMessage := fmt.Sprintf("Unvalid variable type %q", variable.tokenType.String())
			return nil, p.NewTslError(errMessage, pos)
		}

		variable.instruction = *internalInstruction
		variable.name = name
	}

	return variable, nil
}

func (p *Parser) parsePostOperatorStatement(instruction *Instruction, internalCall bool) (*Instruction, error) {
	var err error

	// For each methods split per a DOT
loop:
	for {
		tok, pos, lit := p.ScanIgnoreDOT()

		// Parse valid post operators methods: on
		switch tok {
		case WS, COMMENT:
			nexTok, _, _ := p.ScanIgnoreWhitespace()

			if nexTok != DOT {
				p.Unscan()
				break loop
			}

		case ON:
			if instruction.globalOperator.isIgnoring {
				errMessage := fmt.Sprintf("Conflict with function %q, can't be applied with ignoring function", tok.String())
				return nil, p.NewTslError(errMessage, pos)
			}
			instruction, err = p.parseOnLabels(tok, pos, lit, instruction)
			if err != nil {
				return nil, err
			}

		case IGNORING:
			if !instruction.globalOperator.isOn {
				errMessage := fmt.Sprintf("Conflict with function %q, can't be applied with on function", tok.String())
				return nil, p.NewTslError(errMessage, pos)
			}

			instruction, err = p.parseIgnoringLabels(tok, pos, lit, instruction)
			if err != nil {
				return nil, err
			}

		case GROUPLEFT, GROUPRIGHT:

			if len(instruction.globalOperator.labels) == 0 && len(instruction.globalOperator.ignoring) == 0 {
				errMessage := fmt.Sprintf("Found function %q, this function expects to find on or ignoring function before on current operator", tok.String())
				return nil, p.NewTslError(errMessage, pos)
			}
			if instruction.globalOperator.group.lit != "" {
				errMessage := fmt.Sprintf("Found function %q, or a group method was already defined for this operator", tok.String())
				return nil, p.NewTslError(errMessage, pos)
			}

			if tok == GROUPLEFT {
				left := InternalField{tokenType: GROUPLEFT, lit: "group_left"}
				instruction.globalOperator.group = left
			}
			if tok == GROUPRIGHT {
				left := InternalField{tokenType: GROUPRIGHT, lit: "group_right"}
				instruction.globalOperator.group = left
			}
			instruction, err = p.parseGroupsLabels(tok, pos, lit, instruction)
			if err != nil {
				return nil, err
			}

		case EOF:
			break loop

		case COMMA, RPAREN:
			if internalCall {
				p.Unscan()
				break loop
			}
		default:
			p.Unscan()

			// Parse post methods
			instruction, err = p.parseTimesSeriesOperators(instruction, internalCall)

			if err != nil {
				return nil, err
			}

			break loop
		}
	}
	return instruction, nil

}

func (p *Parser) parsePostSelectStatement(instruction *Instruction, internalCall bool) (*Instruction, error) {
	var err error
	var timeSet bool

	// In case of Select Variable use, check if a time was previously set
	if instruction.hasSelect {
		timeSet = instruction.selectStatement.timeSet
	}

	// For each methods split per a DOT
loop:
	for {
		tok, pos, lit := p.ScanIgnoreDOT()

		// Parse valid post select methods that can be one time method: from or last and an undefinite number of where
		switch tok {

		case NAMES, LABELS, SELECTORS, ATTRIBUTES:

			if internalCall || instruction.isGlobalOperator || instruction.selectStatement.timeSet {
				errMessage := fmt.Sprintf("Function %q, expects to stand on a single select statement", tok.String())
				return nil, p.NewTslError(errMessage, pos)
			}
			instruction, err = p.parseSelectMeta(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}

			nexTok, _, _ := p.ScanIgnoreWhitespace()
			if nexTok == RPAREN || nexTok == EOF {
				break loop
			} else if nexTok != DOT {
				p.Unscan()
				break loop
			} else {
				errMessage := fmt.Sprintf("Function %q, expects to stand on a single select statement", tok.String())
				return nil, p.NewTslError(errMessage, pos)
			}

		case WHERE:
			instruction, err = p.parseWhere(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}

		case ATTRIBUTEPOLICY:
			instruction, err = p.parseAttributePolicy(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}

		case FROM:

			// Verify that current From is first time instruction, otherwise return an error
			if timeSet {
				errMessage := fmt.Sprintf("Found %q function or a time function is already set", tok.String())
				return nil, p.NewTslError(errMessage, pos)
			}
			timeSet = true

			instruction, err = p.parseFrom(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}
		case LAST:
			// Verify that current Last is first time instruction, otherwise return an error
			if timeSet {
				errMessage := fmt.Sprintf("Found %q function or a time function is already set", tok.String())
				return nil, p.NewTslError(errMessage, pos)
			}
			timeSet = true

			instruction, err = p.parseLast(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}
		case WS, COMMENT:
			nexTok, _, _ := p.ScanIgnoreWhitespace()
			if nexTok != DOT {
				p.Unscan()
				break loop
			}
		case EOF:
			break loop

		case COMMA, RPAREN:
			if internalCall {
				p.Unscan()
				break loop
			}
		default:
			p.Unscan()

			// Parse post select methods
			instruction, err = p.parseTimesSeriesOperators(instruction, internalCall)

			if err != nil {
				return nil, err
			}

			break loop
		}
	}
	return instruction, nil

}

func (p *Parser) parseTimesSeriesOperators(instruction *Instruction, internalCall bool) (*Instruction, error) {
	var err error
	hasSampling := false

	// For each methods split per DOT
loop:
	for {
		tok, pos, lit := p.ScanIgnoreDOT()

		// Parse a valid Time series method
		switch tok {

		case SAMPLEBY, SAMPLE:
			instruction, err = p.parseSampleBy(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}

			hasSampling = true

		case ADDSERIES, SUBSERIES, MULSERIES, DIVSERIES, EQUAL, MAXWITH, MINWITH, NOTEQUAL, GREATERTHAN, GREATEROREQUAL, LESSTHAN, LESSOREQUAL, LOGN, SHRINK, KEEPFIRSTVALUES, KEEPLASTVALUES, TIMESCALE:
			instruction, err = p.parseSingleNumericOperator(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}

		case TIMECLIP, TIMEMODULO, TIMESPLIT, QUANTIZE:
			instruction, err = p.parseOperators(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}

		case ANDL, ORL:
			instruction, err = p.parseBooleanOperator(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}

		case SHIFT, RATE:
			instruction, err = p.parseTimeOperator(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}

		case DELTA, MEAN, MEDIAN, MIN, MAX, COUNT, STDDEV, STDVAR, SUM, JOIN, PERCENTILE, FINITE:
			instruction, err = p.parseWindowOperator(tok, pos, lit, instruction, hasSampling)

			if err != nil {
				return nil, err
			}

		case ADDNAMEPREFIX, ADDNAMESUFFIX, RENAME, RENAMEBY, STORE, FILTERBYNAME, FILTERBYLASTVALUE:
			instruction, err = p.parseNStringOperator(tok, pos, lit, 1, instruction)

			if err != nil {
				return nil, err
			}

		case REMOVELABELS, FILTERBYLABELS:
			instruction, err = p.parseNStringOperator(tok, pos, lit, -1, instruction)

			if err != nil {
				return nil, err
			}

		case RENAMELABELKEY:
			instruction, err = p.parseNStringOperator(tok, pos, lit, 2, instruction)

			if err != nil {
				return nil, err
			}

		case RENAMELABELVALUE:
			instruction, err = p.parseRenameLabelValue(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}
		case ABS, CEIL, CUMULATIVESUM, DAY, FLOOR, HOUR, LN, LOG2, LOG10, MINUTE, MONTH, ROUND, RESETS, SQRT, TIMESTAMP, WEEKDAY, YEAR:
			instruction, err = p.parseNoOperator(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}

		case CUMULATIVE, WINDOW:
			instruction, err = p.parseAggregatorFunction(tok, pos, lit, instruction)

			if err != nil {
				return nil, err
			}

		case SORTBY, SORTDESCBY:
			instruction, err = p.parseOperatorBy(tok, pos, lit, instruction, false)
			if err != nil {
				return nil, err
			}

		case BOTTOMNBY, TOPNBY:
			instruction, err = p.parseOperatorBy(tok, pos, lit, instruction, true)
			if err != nil {
				return nil, err
			}

		case SORT, SORTDESC:
			instruction, err = p.parseOperatorBy(tok, pos, lit, instruction, false)
			if err != nil {
				return nil, err
			}

		case BOTTOMN, TOPN:
			instruction, err = p.parseOperatorBy(tok, pos, lit, instruction, true)
			if err != nil {
				return nil, err
			}
		case GROUPBY, GROUP, GROUPWITHOUT:
			instruction, err = p.parseGroupBy(tok, pos, lit, instruction, hasSampling)
			if err != nil {
				return nil, err
			}
		case WS, COMMENT:
			nexTok, _, _ := p.ScanIgnoreWhitespace()
			if nexTok != DOT {
				p.Unscan()
				break loop
			}

		case IDENT:
			p.Unscan()
			break loop

		case EOF:
			break loop

		case COMMA, RPAREN:

			if internalCall {
				p.Unscan()
				break loop
			}

		default:
			errMessage := fmt.Sprintf("Unvalid method found %q, a time series method or end of statement is expected", tokstr(tok, lit))
			return nil, p.NewTslError(errMessage, pos)
		}
	}
	return instruction, nil
}

//
// Individual methods parser
//
func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

// Connect TSL method parser
func (p *Parser) parseConnect(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {

	// Instantiate the CONNECT struct of the instruction as the last CONNECT replace the previous set instruction
	connectStatement := &ConnectStatement{}
	connectStatement.pos = pos
	instruction.connectStatement = *connectStatement

	// Next load all string CONNECT fields, limit to 2: api and token
	fields, err := p.ParseFields(SELECT.String(), map[int][]InternalField{}, 4)

	if err != nil {
		return nil, err
	}

	// Set current connect field into instruction
	instruction.connectStatement.connectType = fields[0].lit

	if instruction.connectStatement.connectType == WARP.String() {

		if len(fields) != 3 {
			return nil, p.NewTslError("error", pos)
		}
		instruction.connectStatement.api = fields[1].lit
		instruction.connectStatement.token = fields[2].lit
	}

	if instruction.connectStatement.connectType == PROM.String() || instruction.connectStatement.connectType == PROMETHEUS.String() {
		if len(fields) == 2 {
			instruction.connectStatement.api = fields[1].lit
			return instruction, nil
		}
		if len(fields) == 4 {
			instruction.connectStatement.api = fields[1].lit
			instruction.connectStatement.token = basicAuth(fields[2].lit, fields[3].lit)
			return instruction, nil
		}
	}

	return instruction, nil
}

// Create TSL method parser, for now accept create a set of Time Series
func (p *Parser) parseCreate(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {
	// Instantiate the Create
	createStatement := &CreateStatement{}
	createStatement.pos = pos
	createStatement.createSeries = make([]CreateSeries, 0)
	instruction.createStatement = *createStatement

	nextTok, _, nextLit := p.ScanIgnoreDOT()

	if nextTok != LPAREN {
		errMessage := fmt.Sprintf("Expect a ( at Create statement, got %q", tokstr(nextTok, nextLit))
		return nil, p.NewTslError(errMessage, pos)
	}

	inCreateMethod := true
	for inCreateMethod {
		createTok, createPos, createLit := p.ScanIgnoreWhitespace()

		switch createTok {
		case COMMA:
			// Do nothing and continue parsing
			continue
		case SERIES:
			var err error
			instruction, err = p.parseCreateSeries(createTok, createPos, createLit, instruction)
			if err != nil {
				return nil, err
			}
		case RPAREN:
			inCreateMethod = false
		default:
			errMessage := fmt.Sprintf("Unvalid method found %q, expect a creation method as series or a closing )", tokstr(createTok, createLit))
			return nil, p.NewTslError(errMessage, createPos)
		}
	}

	return instruction, nil
}

// Parse Create a single series method parser
func (p *Parser) parseCreateSeries(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {
	createSeries := &CreateSeries{}

	createSeries.end = &InternalField{}

	// Load select valid field parse given series name: can be a STRING or IDENT
	zeroFields := []InternalField{{tokenType: IDENT}, {tokenType: ASTERISK}, {tokenType: STRING}}
	selectFields := map[int][]InternalField{0: zeroFields}

	// Next load all select fields, limit to 1
	fields, err := p.ParseFields(SELECT.String(), selectFields, 1)

	if err != nil {
		return nil, err
	}

	// Set current select field into instruction
	if fields[0].tokenType == STRING {
		createSeries.metric = fields[0].lit
	}

	inCreateMethod := true
	for inCreateMethod {
		nextTok, nextPos, nextLit := p.ScanIgnoreDOT()

		switch nextTok {
		case SETLABELS:
			var err error
			instruction, createSeries, err = p.parseCreateSetLabels(nextTok, nextPos, nextLit, instruction, createSeries)
			if err != nil {
				return nil, err
			}
		case SETVALUES:
			var err error
			instruction, createSeries, err = p.parseCreateSetValues(nextTok, nextPos, nextLit, instruction, createSeries)
			if err != nil {
				return nil, err
			}
		case RPAREN, COMMA:
			p.Unscan()
			inCreateMethod = false
			if len(createSeries.values) > 0 {
				instruction.selectStatement.hasFrom = true
				instruction.selectStatement.from = FromStatement{to: InternalField{tokenType: IDENT, lit: "$maxCreateTick"}, hasTo: true, from: InternalField{tokenType: IDENT, lit: "$minCreateTick"}}
			}
			instruction.createStatement.createSeries = append(instruction.createStatement.createSeries, *createSeries)
		}
	}

	if err != nil {
		return nil, err
	}
	return instruction, nil
}

// Parse labels of a created a single series method parser
func (p *Parser) parseCreateSetValues(tok Token, pos Pos, lit string, instruction *Instruction, createSeries *CreateSeries) (*Instruction, *CreateSeries, error) {
	inCreateMethod := true

	nextTok, _, nextLit := p.Scan()

	if nextTok != LPAREN {
		errMessage := fmt.Sprintf("Expect a ( at SetValues statement, got %q", tokstr(nextTok, nextLit))
		return nil, nil, p.NewTslError(errMessage, pos)
	}
	hasEnd := false

	for inCreateMethod {
		nextTok, nextPos, nextLit := p.ScanIgnoreWhitespace()

		switch nextTok {

		case NUMBER, INTEGER, DURATIONVAL, NEGINTEGER, NEGNUMBER:
			createSeries.end = &InternalField{tokenType: nextTok, lit: nextLit}

			if hasEnd {
				errMessage := fmt.Sprintf("single end date value was previously set in setValues, found %q", tokstr(nextTok, nextLit))
				return nil, nil, p.NewTslError(errMessage, nextPos)
			}
			hasEnd = true

			sepTok, sepPos, sepLit := p.ScanIgnoreWhitespace()

			if sepTok == RPAREN {
				p.Unscan()
			} else if sepTok != COMMA {
				errMessage := fmt.Sprintf("Expect a , at SetValues statement, got %q", tokstr(sepTok, sepLit))
				return nil, nil, p.NewTslError(errMessage, sepPos)
			}
		case STRING:
			if nextLit == NowValue.String() {
				createSeries.end = &InternalField{tokenType: nextTok, lit: nextLit}
			} else {
				errMessage := fmt.Sprintf("Unvalid param found in setValue expect or a lastTick long, or now string, or a set of values, found %q", tokstr(nextTok, nextLit))
				return nil, nil, p.NewTslError(errMessage, nextPos)
			}

			if hasEnd {
				errMessage := fmt.Sprintf("single end date value was previously set in setValues, found %q", tokstr(nextTok, nextLit))
				return nil, nil, p.NewTslError(errMessage, nextPos)
			}
			hasEnd = true

			sepTok, sepPos, sepLit := p.ScanIgnoreWhitespace()

			if sepTok == RPAREN {
				p.Unscan()
			} else if sepTok != COMMA {
				errMessage := fmt.Sprintf("Expect a , at SetValues statement, got %q", tokstr(sepTok, sepLit))
				return nil, nil, p.NewTslError(errMessage, sepPos)
			}

		case LBRACKET:
			tickTok, tickPos, tickLit := p.ScanIgnoreWhitespace()

			if tickTok != NUMBER && tickTok != DURATIONVAL && tickTok != INTEGER && tickTok != NEGINTEGER && tickTok != NEGNUMBER {
				errMessage := fmt.Sprintf("Unvalid param found in setValue expect a tick as Number or duration, found %q", tokstr(tickTok, tickLit))
				return nil, nil, p.NewTslError(errMessage, tickPos)
			}

			tickField := &InternalField{tokenType: tickTok, lit: tickLit}

			sepTok, sepPos, sepLit := p.ScanIgnoreWhitespace()

			if sepTok != COMMA {
				errMessage := fmt.Sprintf("Expect a , at SetValues statement between tick and value, got %q", tokstr(sepTok, sepLit))
				return nil, nil, p.NewTslError(errMessage, sepPos)
			}

			valueTok, valuePos, valueLit := p.ScanIgnoreWhitespace()

			if valueTok != NUMBER && valueTok != DURATIONVAL && valueTok != INTEGER && valueTok != STRING && valueTok != NEGINTEGER && valueTok != NEGNUMBER {
				errMessage := fmt.Sprintf("Unvalid param found in setValue expect a tick as Number or duration, found %q", tokstr(valueTok, valueLit))
				return nil, nil, p.NewTslError(errMessage, valuePos)
			}
			valueField := &InternalField{tokenType: valueTok, lit: valueLit}

			dataPoint := DataPoint{tick: tickField, value: valueField}

			createSeries.values = append(createSeries.values, dataPoint)

			endTok, endPos, endLit := p.ScanIgnoreWhitespace()

			if endTok != RBRACKET {
				errMessage := fmt.Sprintf("Expect a closing ] in set Values, got %q", tokstr(endTok, endLit))
				return nil, nil, p.NewTslError(errMessage, endPos)
			}

			sepTok, sepPos, sepLit = p.ScanIgnoreWhitespace()

			if sepTok == RPAREN {
				p.Unscan()
			} else if sepTok != COMMA {
				errMessage := fmt.Sprintf("Expect a , at SetValues statement, got %q", tokstr(sepTok, sepLit))
				return nil, nil, p.NewTslError(errMessage, sepPos)
			}
		case RPAREN:
			inCreateMethod = false
		default:
			errMessage := fmt.Sprintf("Unvalid param found in setValue expect or a lastTick long, or now string, or a set of values, found %q", tokstr(nextTok, nextLit))
			return nil, nil, p.NewTslError(errMessage, nextPos)
		}

	}
	return instruction, createSeries, nil
}

// Parse labels of a created a single series method parser
func (p *Parser) parseCreateSetLabels(tok Token, pos Pos, lit string, instruction *Instruction, createSeries *CreateSeries) (*Instruction, *CreateSeries, error) {
	var err error

	instruction.selectStatement.where = make([]WhereField, 0)
	instruction, err = p.parseWhere(tok, pos, lit, instruction)
	if err != nil {
		return nil, nil, err
	}

	createSeries.where = append(createSeries.where, instruction.selectStatement.where...)
	return instruction, createSeries, nil
}

// Select TSL method parser
func (p *Parser) parseSelect(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {

	// Instantiate the SELECT struct as select can be set only once
	selectStatement := &SelectStatement{metric: "select", attributePolicy: Merge}
	selectStatement.pos = pos
	selectStatement.frameworks = make([]FrameworkStatement, 0)

	if p.hasQueryRange {
		queryRange := p.queryRange
		if queryRange.defaultIsLast {
			// Instantiate a new last statement
			last := &LastStatement{last: queryRange.defaultLastDuration, isDuration: true}
			last.pos = Pos{}
			selectStatement.last = *last
			selectStatement.hasLast = true
		} else {
			// Instantiate a new from statement

			// Instantiate from with fields loaded
			from := &FromStatement{from: queryRange.defaultFromStart}
			from.pos = Pos{}

			if queryRange.defaultFromTo.lit != "" {

				from.to = queryRange.defaultFromTo
				from.hasTo = true
			}
			selectStatement.from = *from
			selectStatement.hasFrom = true
		}
	}

	// Load select valid field: can be a STRING or strictly equals to *
	zeroFields := []InternalField{{tokenType: IDENT}, {tokenType: ASTERISK}, {tokenType: STRING}}
	selectFields := map[int][]InternalField{0: zeroFields}

	// Next load all select fields, limit to 1
	fields, err := p.ParseFields(SELECT.String(), selectFields, 1)

	if err != nil {
		return nil, err
	}

	// Set current select field into instruction
	if fields[0].tokenType == STRING {
		selectStatement.metric = fields[0].lit
	} else if fields[0].tokenType == ASTERISK {
		selectStatement.selectAll = true
	}

	if err != nil {
		return nil, err
	}

	instruction.selectStatement = *selectStatement
	return instruction, nil
}

// TSL selectMeta method parser
func (p *Parser) parseSelectMeta(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {

	op := &FrameworkStatement{
		pos:               pos,
		operator:          tok,
		attributes:        make(map[PrefixAttributes]InternalField),
		unNamedAttributes: make(map[int]InternalField)}

	operatorCount := 0

	if tok == LABELS || tok == ATTRIBUTES {
		operatorCount = 1
	}

	// Load expected fields
	fields, err := p.ParseFields(tok.String(), map[int][]InternalField{}, operatorCount)

	if err != nil {
		return nil, err
	}

	// Validate all received fields
	for index, field := range fields {
		op.unNamedAttributes[index] = field
	}

	instruction.isMeta = true
	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *op)
	return instruction, nil
}

// TSL where method parser
func (p *Parser) parseAttributePolicy(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {

	// Load expected fields
	fields, err := p.ParseFields(tok.String(), map[int][]InternalField{}, 1)

	if err != nil {
		return nil, err
	}

	// Check field size number
	if len(fields) < 1 {
		errMessage := fmt.Sprintf("The %q function expects at least %d %q parameter(s)", tok.String(), 1, STRING.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	switch fields[0].lit {
	case Merge.String():
		instruction.selectStatement.attributePolicy = Merge
	case Split.String():
		instruction.selectStatement.attributePolicy = Split
	case Remove.String():
		instruction.selectStatement.attributePolicy = Remove
	default:
		errMessage := fmt.Sprintf("In %q function expects parameter must be one of %q, %q or %q", tok.String(), Merge.String(), Split.String(), Remove.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	return instruction, nil
}

// TSL where method parser
func (p *Parser) parseWhere(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {

	fieldsList := []InternalField{{tokenType: STRING}, {tokenType: INTERNALLIST}}

	// Load where fields, expects infinite number of fields
	fields, err := p.ParseFields(tok.String(), map[int][]InternalField{0: fieldsList}, -1)
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		errMessage := fmt.Sprintf("Function %q, expects at least one string parameter or a string list", tok.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	if fields[0].tokenType == INTERNALLIST && len(fields) > 1 {
		errMessage := fmt.Sprintf("Function %q, got both a string list and a string parameter", tok.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	// Convert all where field into string field
	fieldsString := make([]WhereField, len(fields))

	if fields[0].tokenType == INTERNALLIST {
		fieldsString = make([]WhereField, len(fields[0].fieldList))

		for k, v := range fields[0].fieldList {
			var err error

			if v.tokenType != STRING {
				errMessage := fmt.Sprintf("Function %q expects only strings as fields clauses", tok.String())
				return nil, p.NewTslError(errMessage, pos)
			}

			where, err := p.getWhereField(v.lit, pos)
			fieldsString[k] = *where
			if err != nil {
				return nil, err
			}
		}

	} else {

		for k, v := range fields {
			var err error

			where, err := p.getWhereField(v.lit, pos)
			fieldsString[k] = *where
			if err != nil {
				return nil, err
			}
		}
	}

	// Append where result into instruction where
	instruction.selectStatement.where = append(instruction.selectStatement.where, fieldsString...)

	return instruction, nil
}

// TSL groups method labels
func (p *Parser) parseGroupsLabels(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {

	// Load where fields, expects infinite number of fields
	fields, err := p.ParseFields(tok.String(), map[int][]InternalField{}, -1)
	if err != nil {
		return nil, err
	}

	labelsMap := map[string]int{}

	for i, label := range instruction.globalOperator.labels {
		labelsMap[label] = i
	}

	// Convert all where field into string field
	fieldsString := make([]string, len(fields))
	for k, v := range fields {
		if v.tokenType != STRING {
			errMessage := fmt.Sprintf("When encounters %q keyword, expects only labels key as %q", tok.String(), STRING.String())
			return nil, p.NewTslError(errMessage, pos)
		}

		_, exists := labelsMap[v.lit]

		if exists {
			errMessage := fmt.Sprintf("In function %q keyword, label %q must not occur in ON and GROUP clause at once", tok.String(), v.lit)
			return nil, p.NewTslError(errMessage, pos)
		}

		fieldsString[k] = v.lit
	}

	// Append where result into instruction where
	instruction.globalOperator.groupLabels = append(instruction.globalOperator.groupLabels, fieldsString...)

	return instruction, nil
}

// TSL ignoring method labels
func (p *Parser) parseIgnoringLabels(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {

	zeroFields := []InternalField{{tokenType: INTERNALLIST}, {tokenType: STRING}}

	// Load where fields, expects infinite number of fields
	fields, err := p.ParseFields(tok.String(), map[int][]InternalField{0: zeroFields}, -1)
	if err != nil {
		return nil, err
	}

	// If current fields is an internal list
	if len(fields) == 1 {
		field := fields[0]

		if field.tokenType == INTERNALLIST {
			fieldsString := make([]string, len(field.fieldList))
			for k, v := range field.fieldList {
				if v.tokenType != STRING {
					errMessage := fmt.Sprintf("When encounters %q keyword, expects only labels key as %q", tok.String(), STRING.String())
					return nil, p.NewTslError(errMessage, pos)
				}
				fieldsString[k] = v.lit
			}
			// Append where result into instruction where
			instruction.globalOperator.ignoring = append(instruction.globalOperator.labels, fieldsString...)

			return instruction, nil
		}
	}

	// Convert all where field into string field
	fieldsString := make([]string, len(fields))
	for k, v := range fields {
		if v.tokenType != STRING {
			errMessage := fmt.Sprintf("When encounters %q keyword, expects only labels key as %q", tok.String(), STRING.String())
			return nil, p.NewTslError(errMessage, pos)
		}
		fieldsString[k] = v.lit
	}

	// Append where result into instruction where
	instruction.globalOperator.ignoring = append(instruction.globalOperator.ignoring, fieldsString...)

	return instruction, nil
}

// TSL on method labels
func (p *Parser) parseOnLabels(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {

	zeroFields := []InternalField{{tokenType: INTERNALLIST}, {tokenType: STRING}}
	selectFields := map[int][]InternalField{0: zeroFields}

	// Load where fields, expects infinite number of fields
	fields, err := p.ParseFields(tok.String(), selectFields, -1)
	if err != nil {
		return nil, err
	}

	// If current fields is an internal list
	if len(fields) == 1 {
		field := fields[0]

		if field.tokenType == INTERNALLIST {
			fieldsString := make([]string, len(field.fieldList))
			for k, v := range field.fieldList {
				if v.tokenType != STRING {
					errMessage := fmt.Sprintf("When encounters %q keyword, expects only labels key as %q", tok.String(), STRING.String())
					return nil, p.NewTslError(errMessage, pos)
				}
				fieldsString[k] = v.lit
			}
			// Append where result into instruction where
			instruction.globalOperator.labels = append(instruction.globalOperator.labels, fieldsString...)

			return instruction, nil
		}
	}

	// Otherwise convert directly all fields into string fields
	fieldsString := make([]string, len(fields))
	for k, v := range fields {
		if v.tokenType != STRING {
			errMessage := fmt.Sprintf("When encounters %q keyword, expects only labels key as %q", tok.String(), STRING.String())
			return nil, p.NewTslError(errMessage, pos)
		}
		fieldsString[k] = v.lit
	}

	// Append where result into instruction where
	instruction.globalOperator.labels = append(instruction.globalOperator.labels, fieldsString...)

	return instruction, nil
}

// Where field validator
func (p *Parser) getWhereField(lit string, pos Pos) (*WhereField, error) {

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
		return nil, p.NewTslError(errMessage, pos)
	}

	return whereField, nil
}

// TSL from method parser
func (p *Parser) parseFrom(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {

	// Load from fields that can be empty or prefixed by "from=" or "to="
	zeroFields := []InternalField{{tokenType: STRING, prefixName: FromFrom, hasPrefixName: true},
		{tokenType: NUMBER, prefixName: FromFrom, hasPrefixName: true},
		{tokenType: INTEGER, prefixName: FromFrom, hasPrefixName: true},
		{tokenType: STRING},
		{tokenType: NUMBER},
		{tokenType: INTEGER}}
	oneFields := []InternalField{{tokenType: STRING, prefixName: FromTo, hasPrefixName: true},
		{tokenType: NUMBER, prefixName: FromTo, hasPrefixName: true},
		{tokenType: INTEGER, prefixName: FromTo, hasPrefixName: true},
		{tokenType: STRING},
		{tokenType: NUMBER},
		{tokenType: INTEGER}}
	fromFields := map[int][]InternalField{0: zeroFields, 1: oneFields}

	fields, err := p.ParseFields(tok.String(), fromFields, 2)
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		errMessage := fmt.Sprintf("Function %q expects at least one parameter, got %q", tok.String(), fields[0].lit)
		return nil, p.NewTslError(errMessage, pos)
	}

	// Instantiate from with fields loaded
	from := &FromStatement{from: fields[0]}
	from.pos = pos

	if len(fields) == 2 {

		from.to = fields[1]
		from.hasTo = true
	}

	instruction.selectStatement.from = *from
	instruction.selectStatement.hasFrom = true
	instruction.selectStatement.hasLast = false
	instruction.selectStatement.timeSet = true
	return instruction, nil
}

// TSL last method parser
func (p *Parser) parseLast(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {

	// Instantiate last method first possible field: expect an INTEGER or a DURATIONVAL
	zeroFields := []InternalField{{tokenType: INTEGER}, {tokenType: DURATIONVAL}}

	// Second and third last method fields are optionals and can be one of timestamp, shift or date
	alternativeFields := []InternalField{
		{tokenType: DURATIONVAL, prefixName: LastShift, hasPrefixName: true},
		{tokenType: INTEGER, prefixName: LastTimestamp, hasPrefixName: true},
		{tokenType: NUMBER, prefixName: LastTimestamp, hasPrefixName: true},
		{tokenType: STRING, prefixName: LastDate, hasPrefixName: true},
		{tokenType: DURATIONVAL},
		{tokenType: INTEGER},
		{tokenType: NUMBER},
		{tokenType: STRING}}

	// Create lastFields variable from generic first and alternative fields arrays
	lastFields := map[int][]InternalField{0: zeroFields, 1: alternativeFields, 2: alternativeFields}

	fields, err := p.ParseFields(LAST.String(), lastFields, 3)
	if err != nil {
		return nil, err
	}

	// Instantiate a new last statement
	last := &LastStatement{}
	last.pos = pos
	last.options = make(map[PrefixAttributes]string)

	// Verify first field
	if fields[0].tokenType == INTEGER {
		last.last = fields[0].lit
	} else if fields[0].tokenType == DURATIONVAL {
		last.last = fields[0].lit
		last.isDuration = true
	} else {
		errMessage := fmt.Sprintf("Function %q expects its first parameter to be an INTEGER or a DURATIONVAL, got %q", tok.String(), fields[0].lit)
		return nil, p.NewTslError(errMessage, pos)
	}

	// When a second or third field are specified, verify them
	if len(fields) > 1 {
		last, err = p.verifyLastFieldsType(fields[1], last, pos)

		if err != nil {
			return nil, err
		}
	}

	if len(fields) > 2 {
		last, err = p.verifyLastFieldsType(fields[2], last, pos)

		if err != nil {
			return nil, err
		}
	}

	instruction.selectStatement.last = *last
	instruction.selectStatement.hasLast = true
	instruction.selectStatement.hasFrom = false
	instruction.selectStatement.timeSet = true
	return instruction, nil
}

// TSL last fields validator
func (p *Parser) verifyLastFieldsType(field InternalField, last *LastStatement, pos Pos) (*LastStatement, error) {

	// Individual last fields validation
	if field.hasPrefixName {
		switch field.prefixName {
		case LastShift, LastTimestamp, LastDate:
			last.options[field.prefixName] = field.lit
		default:
			errMessage := fmt.Sprintf("Function %q expects its second parameter to shift, timestamp or a date, got %q", LAST.String(), field.lit)
			return nil, p.NewTslError(errMessage, pos)
		}
	} else {
		switch field.tokenType {
		case INTEGER:
			last.options[LastTimestamp] = field.lit
		case DURATIONVAL:
			last.options[LastShift] = field.lit
		case STRING:
			last.options[LastDate] = field.lit
		default:
			errMessage := fmt.Sprintf("Function %q expects its second parameter to be a DURATIONVAL shift, an INTEGER timestamp or a STRING date, got %q", LAST.String(), field.lit)
			return nil, p.NewTslError(errMessage, pos)
		}
	}
	return last, nil
}

// parseGlobalSeriesOp method used to parse Time series operators
func (p *Parser) parseGlobalSeriesOp(tok Token, pos Pos, lit string, instruction *Instruction, maxLength int, loadVariable bool) (*Instruction, error) {

	instruction.isGlobalOperator = true
	gOp := &GlobalOperator{operator: tok}
	gOp.pos = pos
	var statements Statements

	// Read field on top
	firstTok, _, _ := p.ScanIgnoreWhitespace()

	if firstTok != LPAREN {
		errMessage := fmt.Sprintf("Operator %q expects parameters", tok.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	index := 0
	for {
		nextTok, nextPos, nextLit := p.ScanIgnoreWhitespace()

		if nextTok == IDENT {
			internalInstruction, err := p.parsePostVariables(nextPos, nextLit, instruction.connectStatement, true)
			if err != nil {
				return nil, err
			}
			statements = append(statements, internalInstruction)
		} else if !(nextTok == COMMA || nextTok == RPAREN) {
			p.Unscan()
			internalInstruction, _, err := p.ParseStatement(&instruction.connectStatement, true, loadVariable)
			if err != nil {
				return nil, err
			}
			statements = append(statements, internalInstruction)
		}

		// If the next token is not a comma or a right ) return an error
		nextTok, nextPos, _ = p.ScanIgnoreWhitespace()
		if !(nextTok == COMMA || nextTok == RPAREN) {
			errMessage := fmt.Sprintf("Expect a , or closing fields with a ), got %q", nextTok.String())
			return nil, p.NewTslError(errMessage, nextPos)
		}

		// Increase parsing index
		index++

		// In case of a limited amount of fields stop the loop earlier with an error
		if maxLength > -1 {
			if index > maxLength {
				errMessage := fmt.Sprintf("Operator %q expects at most %d parameters", tok.String(), maxLength)
				return nil, p.NewTslError(errMessage, pos)
			}
		}

		// And in case of ) stop the loop
		if nextTok == RPAREN {
			break
		}
	}

	if len(statements) >= 2 {
		gOp.instructions = statements
	} else {
		errMessage := fmt.Sprintf("Operator %q expects at least 2 parameters", tok.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	instruction.globalOperator = *gOp

	return instruction, nil
}

// TSL SampleBy method parser
func (p *Parser) parseSampleBy(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {

	sampler := &FrameworkStatement{}
	sampler.pos = pos
	sampler.operator = tok
	sampler.attributes = make(map[PrefixAttributes]InternalField)
	sampler.unNamedAttributes = make(map[int]InternalField)

	spanCountField := []InternalField{
		{tokenType: DURATIONVAL},
		{tokenType: DURATIONVAL, prefixName: SampleSpan, hasPrefixName: true},
		{tokenType: INTEGER, prefixName: SampleAuto, hasPrefixName: true},
		{tokenType: INTEGER},
	}

	spanAggregator := []InternalField{
		{tokenType: STRING, prefixName: SampleAggregator, hasPrefixName: true},
		{tokenType: MEAN},
		{tokenType: MAX},
		{tokenType: FIRST},
		{tokenType: LAST},
		{tokenType: MIN},
		{tokenType: SUM},
		{tokenType: JOIN},
		{tokenType: MEDIAN},
		{tokenType: COUNT},
		{tokenType: ANDL},
		{tokenType: ORL},
		{tokenType: PERCENTILE},
	}

	// Instantiate SambleBy method possible field: expect an Aggregator or an Aggregator Ident
	optionalParameter := []InternalField{
		{tokenType: DURATIONVAL, prefixName: SampleSpan, hasPrefixName: true},
		{tokenType: TRUE, prefixName: SampleRelative, hasPrefixName: true},
		{tokenType: FALSE, prefixName: SampleRelative, hasPrefixName: true},
		{tokenType: STRING, prefixName: SampleFill, hasPrefixName: true},
		{tokenType: FILL, prefixName: SampleFill, hasPrefixName: true},
		{tokenType: INTERNALLIST, prefixName: SampleFill, hasPrefixName: true},
		{tokenType: INTEGER, prefixName: SampleAuto, hasPrefixName: true},
		{tokenType: FILL},
		{tokenType: STRING},
		{tokenType: INTERNALLIST},
		{tokenType: TRUE},
		{tokenType: FALSE},
		{tokenType: NUMBER},
		{tokenType: INTEGER},
	}

	minField := 2
	sampleByFields := map[int][]InternalField{0: spanCountField, 1: spanAggregator, 2: optionalParameter, 3: optionalParameter, 4: optionalParameter, 5: optionalParameter}

	if tok == SAMPLE {
		minField = 1
		sampleByFields = map[int][]InternalField{0: spanAggregator, 1: optionalParameter, 2: optionalParameter, 3: optionalParameter, 4: optionalParameter}
	}
	// Load sampler expected fields
	fields, err := p.ParseFields(tok.String(), sampleByFields, len(sampleByFields))

	if err != nil {
		return nil, err
	}

	if len(fields) < minField {
		errMessage := fmt.Sprintf("The %q method expects at least two parameters: a %q or a %q and a %q", tok.String(), SampleSpan, SampleAuto, SampleAggregator)
		if tok == SAMPLE {
			errMessage = fmt.Sprintf("The %q method expects at least one parameter an aggregator", SampleAggregator)
		}
		return nil, p.NewTslError(errMessage, pos)
	}

	// Index to skip (aggregators parameters)
	skippedIndex := make(map[int]bool)
	// Validate all received fields
	for index, field := range fields {

		// Skip sampleBy aggregator parameter
		if _, exists := skippedIndex[index]; exists {
			continue
		}

		if field.hasPrefixName {
			if field.tokenType == FILL {
				field.prefixName = SampleFillValue
			}
			sampler.attributes[field.prefixName] = field
			continue
		}

		// Validate sampler count or span
		if index == 0 {
			if field.tokenType == DURATIONVAL {
				field.prefixName = SampleSpan
				field.hasPrefixName = true
			} else if field.tokenType == INTEGER {
				field.prefixName = SampleAuto
				field.hasPrefixName = true
			}
			sampler.attributes[field.prefixName] = field
			continue
		}

		// Validate sampler aggregator
		if index == 1 {

			if field.tokenType != STRING && field.tokenType != NUMBER && field.tokenType != INTEGER {
				field.prefixName = SampleAggregator
				field.hasPrefixName = true
				field.lit = field.tokenType.String()
			}

			if field.tokenType == JOIN || field.tokenType == PERCENTILE {

				var err error
				sampler, skippedIndex, err = p.manageValueAggregator(sampler, pos, tok, field, fields, index, skippedIndex)
				if err != nil {
					return nil, err
				}
			}

			sampler.attributes[field.prefixName] = field
			continue
		}

		// Validate sampler optionnals parameter
		if field.tokenType == DURATIONVAL {
			field.prefixName = SampleSpan
			field.hasPrefixName = true

		} else if field.tokenType == FILL {
			field.prefixName = SampleFillValue
			field.hasPrefixName = true
		} else if field.tokenType == STRING {
			field.prefixName = SampleFill
			field.hasPrefixName = true
		} else if field.tokenType == INTERNALLIST {
			field.prefixName = SampleFill
			for _, internalField := range field.fieldList {
				if internalField.tokenType != STRING {
					errMessage := fmt.Sprintf("Exect a valid field string for %q in function %q", SampleFill, tok.String())
					return nil, p.NewTslError(errMessage, pos)
				}

				if !(internalField.lit == Previous.String() || internalField.lit == Next.String() || internalField.lit == Interpolate.String()) {
					errMessage := fmt.Sprintf("Unvalid string %q in function %q, expects one of %q, %q or %q", lit, tok.String(), Previous.String(), Next.String(), Interpolate.String())
					return nil, p.NewTslError(errMessage, pos)
				}
			}
			field.hasPrefixName = true
		} else if field.tokenType == INTEGER {
			field.prefixName = SampleAuto
			field.hasPrefixName = true
		} else if field.tokenType == TRUE || field.tokenType == FALSE {
			field.prefixName = SampleRelative
			field.hasPrefixName = true
			field.lit = field.tokenType.String()
		} else {
			errMessage := fmt.Sprintf("Unexpected field %q in function %q", tokstr(field.tokenType, field.lit), tok.String())
			return nil, p.NewTslError(errMessage, pos)
		}

		sampler.attributes[field.prefixName] = field
	}

	_, hasSpan := sampler.attributes[SampleSpan]
	_, hasCount := sampler.attributes[SampleAuto]

	// Error if span set in sample and fetch not fixed in time
	if hasSpan && !hasCount {
		if !instruction.selectStatement.hasFrom && !instruction.selectStatement.last.isDuration {
			errMessage := fmt.Sprintf("In %q function, got a span when select was done on a counted item. Use also an integer number as sample count in that case", tok.String())
			return nil, p.NewTslError(errMessage, pos)
		}
	}

	if tok == SAMPLE && (hasSpan || hasCount) {
		errMessage := fmt.Sprintf("In %q function, no span or count can be set", tok.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	if tok == SAMPLE {
		if instruction.selectStatement.hasLast && !instruction.selectStatement.last.isDuration {
			errMessage := fmt.Sprintf("In %q function, cannot work on Integer last values", tok.String())
			return nil, p.NewTslError(errMessage, pos)
		}
		sampler.attributes[SampleAuto] = InternalField{tokenType: INTEGER, lit: p.samplersCount}
	}

	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *sampler)
	return instruction, nil
}

// Validate SampleBy field aggregator
func validateSampleByAggregator(types []InternalField, lit string) bool {

	for _, validType := range types {
		if validType.tokenType.String() == lit {
			if validType.tokenType != STRING {
				return true
			}
		}
	}

	return false
}

// GroupBy TSL method parser
func (p *Parser) parseOperatorBy(tok Token, pos Pos, lit string, instruction *Instruction, hasArg bool) (*Instruction, error) {
	opBy := &FrameworkStatement{}
	opBy.pos = pos
	opBy.operator = tok
	opBy.attributes = make(map[PrefixAttributes]InternalField)
	opBy.unNamedAttributes = make(map[int]InternalField)

	// Load first field possible type
	aggregatorFields := []InternalField{
		{tokenType: MEAN},
		{tokenType: MAX},
		{tokenType: LAST},
		{tokenType: FIRST},
		{tokenType: MIN},
		{tokenType: SUM},
		{tokenType: MEDIAN},
		{tokenType: COUNT},
		{tokenType: ANDL},
		{tokenType: ORL},
		{tokenType: PERCENTILE},
		{tokenType: INTEGER, prefixName: Aggregator, hasPrefixName: true},
		{tokenType: INTEGER},
	}

	// Load first field possible type
	paramFields := []InternalField{
		{tokenType: INTEGER, prefixName: Aggregator, hasPrefixName: true},
		{tokenType: INTEGER},
	}

	// Load expected fields
	//if hasBy {

	var err error
	var fields map[int]InternalField

	// Index to skip (aggregators parameters)
	skippedIndex := make(map[int]bool)

	if !hasArg {
		fields, err = p.ParseFields(tok.String(), map[int][]InternalField{0: aggregatorFields, 1: paramFields}, 2)
		if err != nil {
			return nil, err
		}

		if len(fields) > 0 {
			field := fields[0]
			if field.tokenType != INTEGER {
				field.lit = field.tokenType.String()
			}
			opBy.attributes[Aggregator] = field
			if field.tokenType == PERCENTILE {

				var err error
				opBy, _, err = p.manageValueAggregator(opBy, pos, tok, field, fields, 0, skippedIndex)
				if err != nil {
					return nil, err
				}
			} else {
				if len(fields) > 1 {
					errMessage := fmt.Sprintf("%q expects at most 1 field(s)", tok.String())
					return nil, p.NewTslError(errMessage, pos)
				}
			}

		}
	} else {

		fields, err = p.ParseFields(tok.String(), map[int][]InternalField{1: aggregatorFields, 0: paramFields, 2: paramFields}, 3)
		if err != nil {
			return nil, err
		}
		if len(fields) < 1 {
			errMessage := fmt.Sprintf("The %q function expects at least a parameter: the n value number", tok.String())
			return nil, p.NewTslError(errMessage, pos)
		}

		// Validate all received fields
		for index, field := range fields {

			// Skip opBy aggregator parameter
			if _, exists := skippedIndex[index]; exists {
				continue
			}

			if index == 0 && field.tokenType == INTEGER {
				if reZeroOnly.MatchString(lit) {
					errMessage := fmt.Sprintf("The %q function cannot work with %q param, expect a value > 0", tok.String(), lit)
					return nil, p.NewTslError(errMessage, pos)
				}
				opBy.attributes[NValue] = fields[index]
			} else if index == 1 {
				if field.tokenType != STRING {
					field.lit = field.tokenType.String()
				}
				opBy.attributes[Aggregator] = field
				if field.tokenType == PERCENTILE {

					var err error
					opBy, skippedIndex, err = p.manageValueAggregator(opBy, pos, tok, field, fields, index, skippedIndex)
					if err != nil {
						return nil, err
					}
				} else {
					if len(fields) > 2 {
						errMessage := fmt.Sprintf("%q expects at most 2 field(s)", tok.String())
						return nil, p.NewTslError(errMessage, pos)
					}
				}
				continue
			} else {
				errMessage := fmt.Sprintf("The %q function encountered an error when parsing its parameter", tok.String())
				return nil, p.NewTslError(errMessage, pos)
			}
		}
	}

	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *opBy)
	return instruction, nil
}

// GroupBy TSL method parser
func (p *Parser) parseGroupBy(tok Token, pos Pos, lit string, instruction *Instruction, hasSampling bool) (*Instruction, error) {
	groupBy := &FrameworkStatement{
		pos:               pos,
		operator:          tok,
		attributes:        make(map[PrefixAttributes]InternalField),
		unNamedAttributes: make(map[int]InternalField)}

	// Load first field possible type
	aggregatorsFields := []InternalField{
		{tokenType: STRING, prefixName: Aggregator, hasPrefixName: true},
		{tokenType: MEAN},
		{tokenType: MAX},
		{tokenType: MIN},
		{tokenType: SUM},
		{tokenType: JOIN},
		{tokenType: MEDIAN},
		{tokenType: COUNT},
		{tokenType: ANDL},
		{tokenType: ORL},
		{tokenType: JOIN},
		{tokenType: PERCENTILE},
	}

	// Load first fields possible type
	labelsFields := []InternalField{
		{tokenType: INTERNALLIST},
		{tokenType: STRING},
	}

	// Optional parameter
	optionalParams := []InternalField{
		{tokenType: STRING},
		{tokenType: NUMBER},
		{tokenType: INTEGER},
		{tokenType: FALSE, prefixName: KeepDistinct, hasPrefixName: true},
		{tokenType: TRUE, prefixName: KeepDistinct, hasPrefixName: true},
		{tokenType: FALSE},
		{tokenType: TRUE},
	}
	var err error
	var fields map[int]InternalField
	// Load expected fields
	if tok == GROUP {

		fields, err = p.ParseFields(tok.String(), map[int][]InternalField{0: aggregatorsFields, 1: optionalParams, 2: optionalParams}, 3)
		if err != nil {
			return nil, err
		}
		if len(fields) < 1 {
			errMessage := fmt.Sprintf("The %q function expects at least one parameter an aggregator", tok.String())
			return nil, p.NewTslError(errMessage, pos)
		}
	} else {

		fields, err = p.ParseFields(tok.String(), map[int][]InternalField{0: labelsFields, 1: aggregatorsFields, 2: optionalParams, 3: optionalParams}, -1)
		if err != nil {
			return nil, err
		}
		if len(fields) < 2 {
			errMessage := fmt.Sprintf("The %q function expects at least two parameters an aggregator and a label key string or a list of labels key string", tok.String())
			return nil, p.NewTslError(errMessage, pos)
		}

	}

	// Index to skip (aggregators parameters)
	skippedIndex := make(map[int]bool)

	// Validate all received fields
	for index, field := range fields {

		// Skip groupBy aggregator parameter
		if _, exists := skippedIndex[index]; exists {
			continue
		}

		if field.tokenType == INTERNALLIST {
			for index, internalField := range field.fieldList {
				if internalField.tokenType != STRING {
					errMessage := fmt.Sprintf("The %q function expects only label key string", tok.String())
					return nil, p.NewTslError(errMessage, pos)
				}
				groupBy.unNamedAttributes[index] = internalField
			}
		} else if (field.tokenType != STRING && field.tokenType != FALSE && field.tokenType != TRUE) || field.prefixName == Aggregator {
			if field.tokenType != STRING && field.tokenType != NUMBER && field.tokenType != INTEGER {
				field.lit = field.tokenType.String()
			}

			if field.tokenType == JOIN || field.tokenType == PERCENTILE {

				var err error
				groupBy, skippedIndex, err = p.manageValueAggregator(groupBy, pos, tok, field, fields, index, skippedIndex)
				if err != nil {
					return nil, err
				}
			}

			groupBy.attributes[Aggregator] = field
			continue
		} else if field.tokenType == STRING {
			groupBy.unNamedAttributes[0] = field
		} else if field.tokenType == FALSE || field.tokenType == TRUE {
			groupBy.attributes[KeepDistinct] = field
		} else {
			errMessage := fmt.Sprintf("The %q function found an unxpected field %q %q ", tok.String(), field.tokenType.String(), field.lit)
			return nil, p.NewTslError(errMessage, pos)
		}
	}

	// Create group by: previous sampler (last per 1 m)
	if !hasSampling {
		sampler := &FrameworkStatement{}
		sampler.operator = SAMPLEBY
		sampler.attributes = make(map[PrefixAttributes]InternalField)
		sampler.attributes[SampleAggregator] = InternalField{lit: LAST.String()}
		sampler.attributes[SampleSpan] = InternalField{lit: "1m"}

		instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *sampler)
	}

	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *groupBy)
	return instruction, nil
}

func (p *Parser) manageValueAggregator(op *FrameworkStatement, pos Pos, tok Token, field InternalField, fields map[int]InternalField, index int, skippedIndex map[int]bool) (*FrameworkStatement, map[int]bool, error) {

	atType := make(map[Token]string)

	if field.tokenType == JOIN {
		atType[STRING] = "string"
	}

	if field.tokenType == PERCENTILE {
		atType[NUMBER] = "a decimal number"
		atType[INTEGER] = "an integer number"
	}

	typesValues := make([]string, len(atType))

	i := 0
	for _, value := range atType {
		typesValues[i] = value
		i++
	}

	stringType := strings.Join(typesValues, " or ")

	if len(fields) < index+1 {
		errMessage := fmt.Sprintf("In %q method, %q expects one %q parameter", tok.String(), field.tokenType.String(), stringType)
		return nil, nil, p.NewTslError(errMessage, pos)
	}
	nextField := fields[index+1]

	// Check next Type
	if _, exists := atType[nextField.tokenType]; !exists {
		errMessage := fmt.Sprintf("In %q method, %q expects one %q parameter", tok.String(), field.tokenType.String(), stringType)
		return nil, nil, p.NewTslError(errMessage, pos)
	}

	// Validate next field percentile
	if field.tokenType == PERCENTILE {
		if nextField.tokenType == NUMBER {
			numberLit, err := strconv.ParseFloat(nextField.lit, 10)

			if err != nil || numberLit >= 100.0 || numberLit < 0 {
				errMessage := fmt.Sprintf("In %q method, %q expects percentile parameter to be included in [0.0, 100.0[", tok.String(), field.tokenType.String())
				return nil, nil, p.NewTslError(errMessage, pos)
			}
		}
		if nextField.tokenType == INTEGER {
			numberLit, err := strconv.Atoi(nextField.lit)

			if err != nil || numberLit >= 100 || numberLit < 0 {
				errMessage := fmt.Sprintf("In %q method, %q expects percentile parameter to be included in [0, 100[", tok.String(), field.tokenType.String())
				return nil, nil, p.NewTslError(errMessage, pos)
			}
			nextField.lit += ".0"
			nextField.tokenType = NUMBER
		}

	}
	op.unNamedAttributes[0] = nextField

	// Update skip Index
	skippedIndex[index+1] = true
	return op, skippedIndex, nil
}

// TSL operator parser that include a single string as parameter
func (p *Parser) parseNStringOperator(tok Token, pos Pos, lit string, operatorCount int, instruction *Instruction) (*Instruction, error) {
	op := &FrameworkStatement{}
	op.pos = pos
	op.operator = tok
	op.attributes = make(map[PrefixAttributes]InternalField)
	op.unNamedAttributes = make(map[int]InternalField)

	if tok == RENAMEBY {
		operatorCount = -1
	}

	// Load expected fields
	fields, err := p.ParseFields(tok.String(), map[int][]InternalField{}, operatorCount)

	if err != nil {
		return nil, err
	}

	if tok == RENAMEBY {
		operatorCount = 1
	} else if tok == REMOVELABELS {
		operatorCount = 0
	}

	// Check field size number
	if len(fields) < operatorCount {
		errMessage := fmt.Sprintf("The %q function expects at least %d %q parameter(s)", tok.String(), operatorCount, STRING.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	// Validate all received fields
	for index, field := range fields {
		op.unNamedAttributes[index] = field
	}

	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *op)
	return instruction, nil
}

// TSL operator parser that include a single string as parameter
func (p *Parser) parseRenameLabelValue(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {
	op := &FrameworkStatement{}
	op.pos = pos
	op.operator = tok
	op.attributes = make(map[PrefixAttributes]InternalField)
	op.unNamedAttributes = make(map[int]InternalField)

	// Load expected fields
	fields, err := p.ParseFields(tok.String(), map[int][]InternalField{}, 3)

	if err != nil {
		return nil, err
	}

	// Check field size number
	if len(fields) < 2 {
		errMessage := fmt.Sprintf("The %q function expects at least %d %q parameter(s)", tok.String(), 2, STRING.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	// When there is only two fields set ".*" as regExp
	if len(fields) == 2 {
		op.unNamedAttributes[0] = fields[0]
		op.unNamedAttributes[1] = InternalField{tokenType: STRING, lit: ".*"}
		op.unNamedAttributes[2] = fields[1]
	} else {

		// Othrewise field framework with all received fields
		for index, field := range fields {
			op.unNamedAttributes[index] = field
		}

	}

	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *op)
	return instruction, nil
}

// TSL operator parser that includes a Number parameter
func (p *Parser) parseSingleNumericOperator(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {
	op := &FrameworkStatement{}
	op.pos = pos
	op.operator = tok
	op.attributes = make(map[PrefixAttributes]InternalField)

	expectedField := 1

	if tok == KEEPFIRSTVALUES || tok == KEEPLASTVALUES {
		expectedField = 0
	}

	// Load single numeric operator
	zeroFields := []InternalField{
		{tokenType: INTEGER},
		{tokenType: NEGINTEGER},
		{tokenType: NUMBER},
		{tokenType: NEGNUMBER}}

	if tok == SHRINK {
		zeroFields = []InternalField{
			{tokenType: INTEGER}}
	}

	// Load expected fields
	fields, err := p.ParseFields(tok.String(), map[int][]InternalField{0: zeroFields}, 1)

	if err != nil {
		return nil, err
	}

	// Check field size number
	if len(fields) < expectedField {
		errMessage := fmt.Sprintf("The %q function expects at least one %q parameter", tok.String(), NUMBER.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	// Validate all received fields
	for _, field := range fields {
		if !field.hasPrefixName {
			field.prefixName = MapperValue
			field.hasPrefixName = true
		}
		op.attributes[field.prefixName] = field
	}
	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *op)
	return instruction, nil
}

func (p *Parser) parseOperators(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {
	op := &FrameworkStatement{}
	op.pos = pos
	op.operator = tok
	op.attributes = make(map[PrefixAttributes]InternalField)
	op.unNamedAttributes = make(map[int]InternalField)

	paramsField := map[int][]InternalField{}

	// Max number of valid parameters, unlimited (strings when no type specified) by default
	maxFieldLength := -1

	// Max number of valid parameters, 0 by default
	minFieldLength := 0

	switch tok {
	case TIMECLIP:
		paramsField[0] = []InternalField{{tokenType: INTEGER}, {tokenType: NUMBER}}
		paramsField[1] = []InternalField{{tokenType: INTEGER}, {tokenType: NUMBER}}
		maxFieldLength = 2
		minFieldLength = 2
	case TIMEMODULO:
		paramsField[0] = []InternalField{{tokenType: INTEGER}}
		paramsField[1] = []InternalField{{tokenType: STRING}}
		maxFieldLength = 2
		minFieldLength = 2
	case TIMESPLIT:
		paramsField[0] = []InternalField{{tokenType: INTEGER}, {tokenType: DURATIONVAL}}
		paramsField[1] = []InternalField{{tokenType: INTEGER}}
		paramsField[2] = []InternalField{{tokenType: STRING}}
		maxFieldLength = 3
		minFieldLength = 3
	case QUANTIZE:
		paramsField[0] = []InternalField{{tokenType: STRING}}
		paramsField[1] = []InternalField{{tokenType: INTEGER}, {tokenType: NUMBER}, {tokenType: INTERNALLIST}}
		paramsField[2] = []InternalField{{tokenType: INTEGER}, {tokenType: NUMBER}, {tokenType: DURATIONVAL}}
		maxFieldLength = 3
		minFieldLength = 2
	}

	// Load expected fields
	fields, err := p.ParseFields(tok.String(), paramsField, maxFieldLength)

	if err != nil {
		return nil, err
	}

	// Check field size number
	if len(fields) < minFieldLength {
		errMessage := fmt.Sprintf("The %q function expects at least %d parameter(s)", tok.String(), minFieldLength)
		return nil, p.NewTslError(errMessage, pos)
	}

	// Validate all received fields
	for index, field := range fields {
		op.unNamedAttributes[index] = field
	}
	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *op)

	return instruction, nil
}

// TSL operator parser that includes a Boolean parameter
func (p *Parser) parseBooleanOperator(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {
	op := &FrameworkStatement{}
	op.pos = pos
	op.operator = tok
	op.attributes = make(map[PrefixAttributes]InternalField)

	// Load single numeric operator
	zeroFields := []InternalField{
		{tokenType: TRUE},
		{tokenType: FALSE}}

	// Load expected fields
	fields, err := p.ParseFields(tok.String(), map[int][]InternalField{0: zeroFields}, 1)

	if err != nil {
		return nil, err
	}

	// Check field size number
	if len(fields) < 1 {
		errMessage := fmt.Sprintf("The %q function expects at least one boolean parameter", tok.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	// Validate all received fields
	for _, field := range fields {
		if !field.hasPrefixName {
			field.prefixName = MapperValue
			field.hasPrefixName = true
		}
		op.attributes[field.prefixName] = field
	}
	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *op)
	return instruction, nil
}

// TSL operator parser that include a window parameter
func (p *Parser) parseWindowOperator(tok Token, pos Pos, lit string, instruction *Instruction, hasSampling bool) (*Instruction, error) {
	op := &FrameworkStatement{}
	op.pos = pos
	op.operator = tok
	op.attributes = make(map[PrefixAttributes]InternalField)
	op.unNamedAttributes = make(map[int]InternalField)

	paramsField := map[int][]InternalField{}
	index := 0

	if tok == JOIN {
		paramsField[index] = []InternalField{{tokenType: STRING}}
		index++
	}

	if tok == PERCENTILE {
		paramsField[index] = []InternalField{{tokenType: INTEGER}, {tokenType: NUMBER}}
		index++
	}

	addedParams := index

	// Load first window field
	zeroFields := []InternalField{
		{tokenType: INTEGER, prefixName: MapperPre, hasPrefixName: true},
		{tokenType: DURATIONVAL, prefixName: MapperPre, hasPrefixName: true},
		{tokenType: DURATIONVAL, prefixName: MapperSampling, hasPrefixName: true},
		{tokenType: INTEGER},
		{tokenType: DURATIONVAL}}

	paramsField[index] = zeroFields
	index++

	oneFields := []InternalField{
		{tokenType: INTEGER, prefixName: MapperPost, hasPrefixName: true},
		{tokenType: DURATIONVAL, prefixName: MapperPost, hasPrefixName: true},
		{tokenType: INTEGER},
		{tokenType: DURATIONVAL}}

	paramsField[index] = oneFields

	// Load expected fields
	fields, err := p.ParseFields(tok.String(), paramsField, len(paramsField))

	if err != nil {
		return nil, err
	}

	if len(fields) == addedParams {
		litPre := "1"
		if tok == FINITE {
			litPre = "0"
		}
		op.attributes[MapperPre] = InternalField{tokenType: INTEGER, prefixName: MapperPre, hasPrefixName: true, lit: litPre}
	}

	// In case of duration window
	if len(fields) == addedParams+1 {

		field := fields[addedParams]

		if field.tokenType != DURATIONVAL {
			errMessage := fmt.Sprintf("The %q function expects one a sampler (duration) parameter or two a %q and a %q parameters", tok.String(), MapperPre.String(), MapperPost.String())
			return nil, p.NewTslError(errMessage, pos)
		}
		if !field.hasPrefixName {
			field.prefixName = MapperSampling
			field.hasPrefixName = true
		}
		op.attributes[field.prefixName] = field

		// Add a default sampler
		if !hasSampling {
			sampler := &FrameworkStatement{}
			sampler.pos = pos
			sampler.operator = SAMPLEBY
			sampler.attributes = make(map[PrefixAttributes]InternalField)
			sampler.attributes[SampleAggregator] = InternalField{lit: LAST.String()}
			sampler.attributes[SampleSpan] = InternalField{lit: "1m"}

			instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *sampler)
		}
	}

	isduration := false
	// Validate all received fields
	for index, field := range fields {

		if index < addedParams && addedParams == 1 {

			if tok == PERCENTILE {
				if field.tokenType == NUMBER {
					numberLit, err := strconv.ParseFloat(field.lit, 10)

					if err != nil || numberLit >= 100.0 || numberLit < 0 {
						errMessage := fmt.Sprintf("In %q method expects percentile parameter to be included in [0.0, 100.0[", tok.String())
						return nil, p.NewTslError(errMessage, pos)
					}
				}
				if field.tokenType == INTEGER {
					numberLit, err := strconv.Atoi(field.lit)

					if err != nil || numberLit >= 100 || numberLit < 0 {
						errMessage := fmt.Sprintf("In %q method expects percentile parameter to be included in [0, 100[", tok.String())
						return nil, p.NewTslError(errMessage, pos)
					}
					field.lit += ".0"
					field.tokenType = NUMBER
				}

			}
			field.prefixName = MapperValue
			field.hasPrefixName = true
			op.attributes[field.prefixName] = field
			continue
		}

		if index < addedParams {
			op.unNamedAttributes[index] = field
			continue
		}

		if field.tokenType == DURATIONVAL {
			isduration = true
		}

		if !field.hasPrefixName && index == addedParams {
			field.prefixName = MapperPre
			field.hasPrefixName = true
		} else if !field.hasPrefixName && index == addedParams+1 {
			field.prefixName = MapperPost
			field.hasPrefixName = true
		}
		op.attributes[field.prefixName] = field
	}

	if isduration && !hasSampling {
		sampler := &FrameworkStatement{}
		sampler.pos = pos
		sampler.operator = SAMPLEBY
		sampler.attributes = make(map[PrefixAttributes]InternalField)
		sampler.attributes[SampleAggregator] = InternalField{lit: LAST.String()}
		sampler.attributes[SampleSpan] = InternalField{lit: "1m"}

		instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *sampler)
	}

	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *op)
	return instruction, nil
}

// TSL single operator parser
func (p *Parser) parseNoOperator(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {
	op := &FrameworkStatement{}
	op.pos = pos
	op.operator = tok
	op.attributes = make(map[PrefixAttributes]InternalField)

	// Instantiate Mapper with no field expected
	fields, err := p.ParseFields(tok.String(), map[int][]InternalField{}, 0)

	if err != nil {
		return nil, err
	}

	// Check field size number
	if len(fields) > 0 {
		errMessage := fmt.Sprintf("The %q function expects no parameter", tok.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *op)
	return instruction, nil
}

// TSL single operator parser
func (p *Parser) parseAggregatorFunction(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {
	op := &FrameworkStatement{}
	op.pos = pos
	op.operator = tok
	op.attributes = make(map[PrefixAttributes]InternalField)
	op.unNamedAttributes = make(map[int]InternalField)

	minField := 1
	maxField := 1

	// Instantiate a single time oprerator
	zeroFields := []InternalField{
		{tokenType: STRING, prefixName: Aggregator, hasPrefixName: true},
		{tokenType: SUM},
		{tokenType: DELTA},
		{tokenType: MEAN},
		{tokenType: MEDIAN},
		{tokenType: MIN},
		{tokenType: MAX},
		{tokenType: COUNT},
		{tokenType: STDDEV},
		{tokenType: STDVAR},
		{tokenType: FIRST},
		{tokenType: JOIN},
		{tokenType: ANDL},
		{tokenType: ORL},
		{tokenType: PERCENTILE},
		{tokenType: LAST}}

	paramsFields := map[int][]InternalField{0: zeroFields}

	if tok == WINDOW {
		// Load first window field
		preField := []InternalField{
			{tokenType: INTEGER, prefixName: MapperPre, hasPrefixName: true},
			{tokenType: DURATIONVAL, prefixName: MapperPre, hasPrefixName: true},
			{tokenType: DURATIONVAL, prefixName: MapperSampling, hasPrefixName: true},
			{tokenType: INTEGER, prefixName: MapperPost, hasPrefixName: true},
			{tokenType: DURATIONVAL, prefixName: MapperPost, hasPrefixName: true},
			{tokenType: INTEGER, prefixName: MapperOccurences, hasPrefixName: true},
			{tokenType: INTEGER},
			{tokenType: STRING},
			{tokenType: DURATIONVAL}}

		paramsFields[1] = preField
		paramsFields[2] = preField
		paramsFields[3] = preField
		maxField = 4
	}

	// Instantiate Mapper with no field expected
	fields, err := p.ParseFields(tok.String(), paramsFields, maxField)

	if err != nil {
		return nil, err
	}

	// Check field size number
	if len(fields) < minField {
		errMessage := fmt.Sprintf("The %q function expects at least %d parameter", tok.String(), minField)
		return nil, p.NewTslError(errMessage, pos)
	}

	if tok == CUMULATIVE {
		op.attributes[MapperPre] = InternalField{lit: "max.tick.sliding.window", tokenType: INTEGER}
	} else {
		op.attributes[MapperPre] = InternalField{lit: "0", tokenType: INTEGER}
	}
	op.attributes[MapperPost] = InternalField{lit: "0", tokenType: INTEGER}

	// Index to skip (aggregators parameters)
	skippedIndex := make(map[int]bool)

	// Add index keep track of where we are of "unamed params" pre and post
	addIndex := 0

	for index, field := range fields {

		// Skip aggregator parameter
		if _, exists := skippedIndex[index]; exists {
			continue
		}
		addIndex++

		if field.hasPrefixName {
			op.attributes[field.prefixName] = field
			continue
		}

		if index == 0 {
			op.attributes[Aggregator] = field

			if field.tokenType == JOIN || field.tokenType == PERCENTILE {

				var err error
				op, skippedIndex, err = p.manageValueAggregator(op, pos, tok, field, fields, index, skippedIndex)
				if err != nil {
					return nil, err
				}
			}
		} else if addIndex == 2 {
			field.prefixName = MapperPre
			field.hasPrefixName = true
			op.attributes[MapperPre] = field
		} else if addIndex == 3 {
			field.prefixName = MapperPost
			field.hasPrefixName = true
			op.attributes[MapperPost] = field
		}
	}

	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *op)
	return instruction, nil
}

// TSL operator parser that includes a Duration time parameter
func (p *Parser) parseTimeOperator(tok Token, pos Pos, lit string, instruction *Instruction) (*Instruction, error) {
	op := &FrameworkStatement{}
	op.pos = pos
	op.operator = tok
	op.attributes = make(map[PrefixAttributes]InternalField)

	minSize := 1

	if tok == RATE {
		minSize = 0
	}

	// Instantiate a single time oprerator
	zeroFields := []InternalField{
		{tokenType: DURATIONVAL}}

	// Load sampler expected fields
	fields, err := p.ParseFields(tok.String(), map[int][]InternalField{0: zeroFields}, 1)

	if err != nil {
		return nil, err
	}

	// Check field size number
	if len(fields) < minSize {
		errMessage := fmt.Sprintf("The %q function expects at least one %q parameter", tok.String(), DURATIONVAL.String())
		return nil, p.NewTslError(errMessage, pos)
	}

	// Validate all received fields
	for _, field := range fields {
		if !field.hasPrefixName {
			field.prefixName = MapperValue
			field.hasPrefixName = true
		}
		op.attributes[field.prefixName] = field
	}

	// When met a Rate token add info to be able to apply a specify Handler for last Missing value in Sampling
	if tok == RATE {
		instruction.selectStatement.hasRate = true
	}
	instruction.selectStatement.frameworks = append(instruction.selectStatement.frameworks, *op)
	return instruction, nil
}

//
// Global fields parser method
//

// ParseFields global fields loader, stop when expectedLength is reached (to an inifinite number of fields set it to -1)
func (p *Parser) ParseFields(function string, internalFields map[int][]InternalField, expectedLength int) (map[int]InternalField, error) {

	// Start field index counter
	index := 0

	res := make(map[int]InternalField)

	// Remove starting "("
	if tok, pos, lit := p.Scan(); tok != LPAREN {
		errMessage := fmt.Sprintf("Expect a (, got %q", tokstr(tok, lit))
		return nil, p.NewTslError(errMessage, pos)
	}

	okField := make([]InternalField, 0)

	// Nex loop over all our comma-delimited fields
	for {

		// Read field on top
		tok, pos, lit := p.ScanIgnoreWhitespace()

		// Stop directly if no fields are set
		if tok == RPAREN {
			break
		}

		// Otherwise load existing field
		if val, ok := internalFields[index]; ok {

			okFieldPrefix := make([]InternalField, 0)
			hasPrefix := false

			// prefix
			for _, field := range val {

				// If the current field can start with an IDENT prefix check item on top
				if field.hasPrefixName && (lit == field.prefixName.String() || tok.String() == field.prefixName.String()) {
					okFieldPrefix = append(okFieldPrefix, field)
					hasPrefix = true
				} else if !field.hasPrefixName {
					okField = append(okField, field)
				}

			}

			isEq, _, _ := p.ScanIgnoreWhitespace()

			p.Unscan()

			isPrefix := tok == FILL && isEq == EQ

			if hasPrefix && isPrefix {

				// remove the allowed prefix
				tok, pos, lit = p.ScanIgnoreWhitespace()

				// Check if we now have the = signs on top
				if tok != EQ {
					errMessage := fmt.Sprintf("found %q, to prefix for %q expects to be followed by an = sign", lit, function)
					return nil, p.NewTslError(errMessage, pos)
				}

				// Load current field value
				tok, pos, lit = p.ScanIgnoreWhitespace()

				okField = okFieldPrefix
			}
		} else {
			okField = make([]InternalField, 1)
			okField[0] = InternalField{tokenType: STRING}
		}

		// Find the current field type
		findType := false

		// Verify that tok type does exists
		for _, field := range okField {

			if tok == IDENT {

				variable, exists := p.variables[lit]
				if !exists {
					errMessage := fmt.Sprintf("Error when parsing %q in %q function, this variable isn't declared", lit, function)
					return nil, p.NewTslError(errMessage, pos)
				}

				if variable.tokenType == field.tokenType {

					if variable.tokenType == INTERNALLIST {
						field.fieldList = variable.fieldList
						tok, _, _ := p.ScanIgnoreWhitespace()

						if tok == DOT {
							var err error
							field.fieldList, err = p.parsePostList(function, field)
							if err != nil {
								return nil, err
							}
						} else {
							p.Unscan()
						}
					}
					field.lit = variable.lit
					findType = true

					// Set current field
					res[index] = field
					break
				}

			} else if tok == LBRACKET && field.tokenType == INTERNALLIST {

				// Load an Internal list fields
				findType = true
				internalList, err := p.ParseInternalFieldList(function, field)
				field = *internalList
				if err != nil {
					return nil, err
				}
				res[index] = field
				break
			} else if tok == FILL && field.tokenType == FILL {
				findType = true
				tok, pos, lit = p.ScanIgnoreWhitespace()
				if tok != LPAREN {
					errMessage := fmt.Sprintf("'fill()' method expected an opening '(', got %q", tokstr(tok, lit))
					return nil, p.NewTslError(errMessage, pos)
				}
				tok, pos, lit = p.ScanIgnoreWhitespace()
				if tok == STRING {
					field.lit = "'" + lit + "'"
				} else if tok == NUMBER || tok == INTEGER {
					field.lit = lit
				} else if tok == TRUE || tok == FALSE {
					field.lit = tok.String()
				} else {
					errMessage := fmt.Sprintf("Unexpected type of field for the 'fill()' method. Expect a native type, got %q", tokstr(tok, lit))
					return nil, p.NewTslError(errMessage, pos)
				}

				tok, pos, lit = p.ScanIgnoreWhitespace()
				if tok != RPAREN {
					errMessage := fmt.Sprintf("'fill()' method expected a closing ')', got %q", tokstr(tok, lit))
					return nil, p.NewTslError(errMessage, pos)
				}
				res[index] = field
				break
			} else if tok == field.tokenType {

				field.lit = lit
				findType = true

				// Set current field
				res[index] = field
				break
			}
		}

		// Verify that item on top has a valid type
		if !findType {
			errMessage := fmt.Sprintf("Found %q, %q does not expected a field with type %q", lit, function, tok.String())
			return nil, p.NewTslError(errMessage, pos)
		}

		tok, pos, lit = p.ScanIgnoreWhitespace()

		// If the next token is not a comma or a right ) return an error
		if !(tok == COMMA || tok == RPAREN) {
			errMessage := fmt.Sprintf("Expect a , or closing fields with a ), got %q", tokstr(tok, lit))
			return nil, p.NewTslError(errMessage, pos)
		}

		// Increase parsing index
		index++

		// In case of a limited amount of fields stop the loop earlier with an error
		if expectedLength > -1 {
			if index > expectedLength {
				errMessage := fmt.Sprintf("%q expects at most %d field(s)", function, expectedLength)
				return nil, p.NewTslError(errMessage, pos)
			}
		}

		// And in case of ) stop the loop
		if tok == RPAREN {
			break
		}
	}
	return res, nil
}

// ParseInternalFieldList Parse an internal list split per COMMA
func (p *Parser) ParseInternalFieldList(function string, field InternalField) (*InternalField, error) {
	field.fieldList = make([]InternalField, 0)

	for {
		var pos Pos

		// Read field on top
		tok, _, lit := p.ScanIgnoreWhitespace()

		// Stop directly if no fields are set
		if tok == RBRACKET {
			break
		}

		field.fieldList = append(field.fieldList, InternalField{tokenType: tok, lit: lit})

		tok, pos, lit = p.ScanIgnoreWhitespace()

		// If the next token is not a comma or a right ] return an error
		if !(tok == COMMA || tok == RBRACKET) {
			errMessage := fmt.Sprintf("Expect a , or closing list fields with a ], got %q", tokstr(tok, lit))
			return nil, p.NewTslError(errMessage, pos)
		}

		// And in case of ] stop the loop
		if tok == RBRACKET {
			break
		}
	}

	tok, _, _ := p.ScanIgnoreWhitespace()

	if tok == DOT {
		var err error
		field.fieldList, err = p.parsePostList(function, field)
		if err != nil {
			return nil, err
		}
	} else {
		p.Unscan()
	}
	return &field, nil
}

// parsePostList Parse post list function
func (p *Parser) parsePostList(function string, field InternalField) ([]InternalField, error) {

	for {
		tok, pos, lit := p.ScanIgnoreWhitespace()

		// Parse first instruction methods, that can be or CONNECT or SELECT
		switch tok {

		case ADDSERIES:
			var err error
			field.fieldList, err = p.parseListAdd(tok, pos, lit, field.fieldList)
			if err != nil {
				return nil, err
			}

		case REMOVE:
			var err error
			field.fieldList, err = p.parseListRemove(tok, pos, lit, field.fieldList)
			if err != nil {
				return nil, err
			}
		}

		nexTok, _, _ := p.ScanIgnoreWhitespace()

		if !(nexTok == DOT) {
			p.Unscan()
			break
		}
	}

	return field.fieldList, nil
}

func (p *Parser) parseListAdd(tok Token, pos Pos, lit string, fields []InternalField) ([]InternalField, error) {

	// Instantiate a single time oprerator
	zeroFields := []InternalField{
		{tokenType: STRING}}

	// Load sampler expected fields
	getAddFields, err := p.ParseFields(tok.String(), map[int][]InternalField{0: zeroFields}, 1)
	if err != nil {
		return nil, err
	}

	newfields := append(fields, getAddFields[0])

	return newfields, nil
}

func (p *Parser) parseListRemove(tok Token, pos Pos, lit string, fields []InternalField) ([]InternalField, error) {

	// Instantiate a single time oprerator
	zeroFields := []InternalField{
		{tokenType: STRING}}

	// Load sampler expected fields
	getAddFields, err := p.ParseFields(tok.String(), map[int][]InternalField{0: zeroFields}, 1)
	if err != nil {
		return nil, err
	}

	for i, item := range fields {
		if item.lit == getAddFields[0].lit {
			newfields := append(fields[:i], fields[i+1:]...)
			return newfields, nil
		}
	}

	return fields, nil

}
