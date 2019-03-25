package tsl

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	lineStartHeader     = "TSL-Line-Start"
	queryRandeHeader    = "TSL-Query-Range"
	samplersCountHeader = "TSL-Samplers"
)

// Request main syntax
type Request struct {
	API   string `json:"api,omitempty"`
	Token string `json:"token,omitempty"`
	Body  string `json:"body,omitempty"`
}

// GetTokenFromBasicAuth is fetching the token for an HTTP Request
func GetTokenFromBasicAuth(request *http.Request) string {
	// Getting token from BasicAuth
	s := strings.SplitN(request.Header.Get("Authorization"), " ", 2)
	if len(s) != 2 {
		return ""
	}
	b, err := base64.StdEncoding.DecodeString(s[1])
	if err != nil {
		return ""
	}
	pair := strings.SplitN(string(b), ":", 2)
	if len(pair) != 2 {
		return ""
	}

	return pair[1]
}

// Query is the main API call method to start parsing Tsl queries
func (tsl Tsl) Query(ctx echo.Context) error {

	tsl.ReqCounter.Inc()

	// Get Header line Value to set from which line query error are count
	lineHeader := ctx.Request().Header.Get(lineStartHeader)

	// Get Header query range value
	queryRange := ctx.Request().Header.Get(queryRandeHeader)

	// Get Header query samplers count
	samplersCount := ctx.Request().Header.Get(samplersCountHeader)

	lineStart := 0

	if lineHeader != "" {
		var err error
		lineStart, err = strconv.Atoi(lineHeader)
		if err != nil {
			tsl.WarnCounter.Inc()
			return ctx.JSON(http.StatusBadRequest, NewError(errors.New("unvalid header "+lineStartHeader+", expects an integer number")))
		}
	}

	// Read user body
	body, err := ioutil.ReadAll(ctx.Request().Body)

	if err != nil {
		tsl.WarnCounter.Inc()
		return ctx.JSON(http.StatusBadRequest, NewError(err))
	}

	// Get Body as logger.info
	log.Debug(string(body))

	// Get default backend URI and USER token
	backendURL := viper.GetString("tsl.default.endpoint")
	tokenString := GetTokenFromBasicAuth(ctx.Request())

	// Get query parsing result
	parser, err := NewParser(strings.NewReader(string(body)), backendURL, tokenString, lineStart, queryRange, samplersCount)
	if err != nil {
		tsl.WarnCounter.Inc()
		return ctx.JSON(http.StatusBadRequest, NewError(err))
	}

	query, err := parser.Parse()

	if err != nil {
		tsl.WarnCounter.Inc()
		return ctx.JSON(http.StatusBadRequest, NewError(err))
	}

	// Get pivot format info
	log.Debug(query.String())

	// Create an instructions map per different back-end to call
	instructionsPerAPI := map[string][]Instruction{}
	for _, instruction := range query.Statements {
		if instructionSet, ok := instructionsPerAPI[instruction.connectStatement.api]; ok {
			instructionSet = append(instructionSet, *instruction)
			instructionsPerAPI[instruction.connectStatement.api] = instructionSet
		} else {
			instructionsPerAPI[instruction.connectStatement.api] = []Instruction{*instruction}
		}
	}

	// Output query buffer
	var buffer bytes.Buffer

	// Set a common now for all Prometheus endpoints
	now := time.Now().UTC()

	// Execute all Warp requests
	warpEndpoints := viper.GetStringSlice("tsl.warp10.endpoints")

	allowAuthenticate := viper.GetBool("tsl.warp10.authenticate")

	for _, warp := range warpEndpoints {

		if instructions, ok := instructionsPerAPI[warp]; ok {

			res, err := warpQuery(instructions, warp, ctx, lineStart, allowAuthenticate)

			if err != nil {
				tsl.ErrCounter.Inc()
				tsl.WarnCounter.Inc()
				return err
			}
			buffer.WriteString(res)
			buffer.WriteString("\n")
		}
	}

	// Execute all Prom requests
	promEndpoints := viper.GetStringSlice("tsl.promql.endpoints")

	for _, prom := range promEndpoints {

		if instructions, ok := instructionsPerAPI[prom]; ok {

			res, err := promQuery(instructions, prom, ctx, now, lineStart)

			if err != nil {
				tsl.ErrCounter.Inc()
				tsl.WarnCounter.Inc()
				return err
			}
			buffer.WriteString(res)
			buffer.WriteString("\n")
		}
	}

	// By default return an empty array
	if buffer.String() == "" {
		buffer.WriteString("[]")
	}

	return ctx.String(http.StatusOK, buffer.String())
}

// Execute all Prom requests on a prometheus backend
func promQuery(instructions []Instruction, prom string, ctx echo.Context, now time.Time, lineStart int) (string, error) {

	var buffer bytes.Buffer

	buffer.WriteString("[")

	prefix := ""
	promRequests := make([]*Ql, len(instructions))
	for index, instruction := range instructions {

		log.Debug(instruction)
		protoParser := ProtoParser{name: "prometheus", lineStart: lineStart}
		promQl, err := protoParser.GeneratePromQl(instruction, now)
		if err != nil {
			log.WithError(err).Error("Could not generate PromQL")
			return "", ctx.JSON(http.StatusMethodNotAllowed, NewError(err))
		}

		promRequests[index] = promQl
	}

	for _, promQl := range promRequests {
		buffer.WriteString(prefix)
		if promQl.Query != "" {
			log.Debug(promQl)
			body, err := execProm(promQl, ctx, prom)
			if err != nil {
				return "", ctx.JSON(http.StatusInternalServerError, NewError(err))
			}
			buffer.WriteString(body)
			buffer.WriteString("\n")
			prefix = ", "
		}
	}
	buffer.WriteString("]")

	return buffer.String(), nil
}

// Execute a Warp 10 request
func warpQuery(instructions []Instruction, warp string, ctx echo.Context, lineStart int, allowAuthenticate bool) (string, error) {

	protoParser := ProtoParser{name: "warp 10", lineStart: lineStart}
	warpscript, err := protoParser.GenerateWarpScript(instructions, allowAuthenticate)
	if err != nil {
		return "", ctx.JSON(http.StatusBadRequest, NewError(err))
	}
	log.Debug(warpscript)
	req := &Request{}
	req.Body = warpscript
	res, err := exec(req, warp, ctx)
	if err != nil {
		log.WithError(err).Error("Could not execute WarpScript")
		return "", ctx.JSON(http.StatusInternalServerError, NewError(err))
	}
	return res, nil
}

// Execute WarpScript on Warp10 metrics backend
func exec(req *Request, warp string, ctx echo.Context) (string, error) {

	if err := ctx.Request().Body.Close(); err != nil {
		log.WithError(err).Error("Cannot close the request body")
		return "", err
	}

	httpReq, err := http.NewRequest(http.MethodPost, warp+"/api/v0/exec", strings.NewReader(req.Body))
	httpReq.Header.Add("User-Agent", "tsl/"+viper.GetString("version")+" (Warp10)")

	if err != nil {
		return "", err
	}

	res, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return "", err
	}

	if http.StatusOK != res.StatusCode {
		return "", errors.New(res.Header.Get("X-Warp10-Error-Message"))
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(res.Body)
	return buf.String(), nil
}

// Execute PromQL on prometheus metrics backend
func execProm(req *Ql, ctx echo.Context, prom string) (string, error) {

	queryType := "query_range"

	if req.InstantQuery {
		queryType = "query"
	}
	u, err := url.Parse(
		fmt.Sprintf("%s/api/v1/%s?query=%s&start=%s&end=%s&step=%s",
			prom,
			queryType,
			url.QueryEscape(req.Query),
			url.QueryEscape(req.Start),
			url.QueryEscape(req.End),
			url.QueryEscape(req.Step)))

	if err != nil {
		return "", err
	}

	httpReq, err := http.NewRequest("GET", u.String(), nil)
	if req.Token != "" {
		httpReq.Header.Add("Authorization", "Basic "+req.Token)
	}

	if err != nil {
		return "", err
	}

	res, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return "", err
	}

	defer res.Body.Close()

	buf := new(bytes.Buffer)
	buf.ReadFrom(res.Body)

	if res.StatusCode != http.StatusOK {
		//return "",
		var message PromError
		json.Unmarshal(buf.Bytes(), &message)
		return buf.String(), errors.New("Fail to execute Prom request: " + message.Error)
	}

	return buf.String(), nil
}

// PromError Internal prom error message, loaded internally only on error
type PromError struct {
	Status    string   `json:"status,omitempty"`
	Data      PromData `json:"data,omitempty"`
	ErrorType string   `json:"errorType,omitempty"`
	Error     string   `json:"error,omitempty"`
}

// PromData Internal prom error data, loaded internally only on error
type PromData struct {
	ResultType string `json:"resultType,omitempty"`
	Result     string `json:"result,omitempty"`
}
