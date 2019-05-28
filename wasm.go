package main

//
// This is the main use to run the TLS interpreter with WASM.
// The method tslToWarpScript is exported to the JavaScript.
//

import (
	"./tsl"
	"bytes"
	"strings"
	"syscall/js"
)

// tslToWarpScriptWasm method to generate WarpScript from TSL statements in WASM and return result with a callback
func tslToWarpScriptWasm(this js.Value, inputs []js.Value) interface{} {
	tslQuery := inputs[0].String()
	defaulToken := inputs[1].String()
	allowAuthenticate := inputs[2].Bool()
	callback := inputs[len(inputs)-1:][0]

	// Get query parsing result
	parser, err := tsl.NewParser(strings.NewReader(tslQuery), "warp", defaulToken, 0, "", "")
	if err != nil {
		callback.Invoke(err.Error(), js.Null())
		return nil
	}

	query, err := parser.Parse()
	if err != nil {
		callback.Invoke(err.Error(), js.Null())
		return nil
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
		callback.Invoke(err.Error(), js.Null())
		return nil
	}

	buffer.WriteString(warpscript)
	buffer.WriteString("\n")
	// By default return an empty array
	if buffer.String() == "" {
		buffer.WriteString("[]")
	}

	callback.Invoke(js.Null(), buffer.String())

	return nil
}

func main() {
	// Go takes a different approach on WASM compared with Rust/C++.
	// Go treats this as an application, meaning that you start a Go runtime, it runs, then exits and you can’t interact with it.
	// If we want to be able to call stuff, but the runtime want to shut down we have to use this channel tricks in order to call
	// tslTowarpscript method as many times as we like.
	c := make(chan bool)
	js.Global().Set("tslToWarpScript", js.FuncOf(tslToWarpScriptWasm))
	<-c
}
