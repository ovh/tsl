package main

//
// This is the main use to run the TLS interpreter with WASM.
// The method tslToWarpScript is exported to the JavaScript.
//

import "C"

import (
	"bytes"
	"strings"

	//"syscall/js"

	"github.com/ovh/tsl/tsl"
)

//export TslToWarpScript
func TslToWarpScript(tslQuery string, token string, allowAuthenticate bool, lineStart int, defaultTimeRange string, defaultSamplers string, nativeVariable string) *C.char {

	variables := strings.Split(nativeVariable, ",")

	// Get query parsing result
	parser, err := tsl.NewParser(strings.NewReader(tslQuery), "warp10", token, lineStart, defaultTimeRange, defaultSamplers, variables)

	if err != nil {
		return C.CString("error - " + err.Error())
	}

	query, err := parser.Parse()
	if err != nil {
		return C.CString("error - " + err.Error())
	}

	// Output query buffer
	var buffer bytes.Buffer

	instructions := []tsl.Instruction{}

	for _, instruction := range query.Statements {
		instructions = append(instructions, *instruction)
	}

	protoParser := tsl.ProtoParser{Name: "warp 10", LineStart: 0}
	warpscript, err := protoParser.GenerateWarpScript(instructions, allowAuthenticate)

	if err != nil {
		return C.CString("error - " + err.Error())
	}

	buffer.WriteString(warpscript)
	// By default return an empty array
	if buffer.String() == "" {
		buffer.WriteString("[]")
	}
	buffer.WriteString("\n")

	return C.CString(buffer.String())
}

func main() {}
