import go from "./wasm_exec.js"

function loadTslWasm(wasmFilePath, callback) {
    WebAssembly.instantiateStreaming(fetch(wasmFilePath), go.importObject).then(callback);
}

export default {
    loadTslWasm,
}
