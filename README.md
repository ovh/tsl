# Time Series Language (TSL)

> TSL is a HTTP proxy which generate [_WarpScript_](https://www.warp10.io) or a [_PromQl script_](https://prometheus.io/docs/prometheus/latest/querying/basics/) based on a TSL query, then execute it on a _Warp 10_ or a _Prometheus_ backend. To get more information about a TSL query, you can have a look at our [initial spec](./spec/spec.md) and our [query doc](./spec/doc.md). Our ANTLR grammar definition is available in the antlr folder [here](./antlr/README.md).

![TSL: a Time Series Language](./assets/logo.png)

## Pre-install

To build and install Tsl you need:

- A working [_go_](https://golang.org) install, with the `GOROOT`and `GOPATH` variables correctly set
- `make` in order to use the `Makefile`

## Install

1. Install the tooling

   `make init`

2. Grab the dependencies

   `make dep`

3. Local build of TSL

   `make dev`

3. Release build of TSL

   `make release`

Binary is now available in the directory `build` under the name `tsl`.

## Configure

TSL needs a [YAML](http://yaml.org/) configuration file with two entries:

- `tsl.default.endpoint`
- `tsl.default.type`

This is an example with a **Warp 10** backend.

```yaml
tsl:
  default:
    endpoint: http//example.com
    type: 'warp10'
```

TSL will look for a `config.yml` file on:

- `/etc/tsl/`
- `$HOME/.tsl`
- the current path

You can also use the `-c` to set a configuration path for a TSL instance.

Without a configuaration file, TSL will use `http://127.0.0.1:9090` as default endpoint.

To use TSL on several backends you can specify the following optional parameters:

```YAML
tsl:
  warp10:
    endpoints:
      - http://127.0.0.1:8080
      - http://127.0.0.1:8081

  promQL:
    endpoints:
      - http://127.0.0.1:9090
      - http://127.0.0.1:9091
```

## Run TSL

You can simply run the TSL binary, `./build/tsl`.

By default, TSL listens on `127.0.0.1:8080`.

```sh
$ ./build/tsl --config path/to/config.yml
INFO[0000] Start tsl server on 127.0.0.1:8080
```

## Query TSL

To send a TSL request, you can use the `v0/query` api endpoint to send TSL queries.

A TSL query can be:

```tsl
select("sys.cpu.nice").where("host=web01").from(1346846400000,to=1346847000005)
```

This is a first TSL query, with the following methods used:

- **select** to specify a metric name to retrieve (or pattern name).
- **where** to set labels that the current Time-series must have.
- **from** to select data between two dates.

You can send this request through an HTTP Post with the cURL Command line or any other HTTP tools like [Insomnia](https://insomnia.rest/) or [PostMan](https://www.getpostman.com/).

As example, we can write a new TSL file containing the previous script and send the cURL command below:

```
curl -v --data-binary @select.tsl 'https://user:password@127.0.0.1:8080/v0/query'
```
This command provides as result a time-series JSON list.

TSL implements a lot of diffetent methods and you can find a more details in the [spec folder](./spec/doc.md).

## Usage

If you need more complex options, use `./build/tsl --help`:

```sh
$ ./build/tsl --help
A proxy that translates queries for a TSDB backend

Usage:
  tsl [flags]
  tsl [command]

Available Commands:
  help        Help about any command
  version     Print the version number

Flags:
  -c, --config string   config file (default is $HOME/.tsl.yaml)
  -h, --help            help for tsl
  -l, --listen string   listen address (default "127.0.0.1:8080")
  -v, --verbose         verbose output

Use "tsl [command] --help" for more information about a command.
```

## Use TSL with WebAssembly

NOTE: A Go 1.11 (> go1.11.1) version at least is needed. Building tsl.wasm works with go 1.12.5.

You can run the TSL to WarpScript interpreter in a WebAssembly program.
The `tsl.wasm` file export the method `tslToWarpScript` callable from JavaScript running in a browser or NodeJS.

To compile the `tsl` package for the web:

`$ make wasm`

As example, you can call the `tslToWarpScript` from a JavaScript program like that:

```javascript
tslToWarpScript("select(\"sys.cpu.nice\").where(\"host=web01\").from(1346846400000,to=1346847000005)", "", false, (err, res) => {
	console.log(res) // Output the tsl query in a WarpScript syntax
})
```

To integrate the `tsl.wasm` file in your program, you should follow the Golang WebAssembly [wiki](https://github.com/golang/go/wiki/WebAssembly).

## Build TSL .so file to use TSL in other language

You can build TSL so library files, usable in other progamming language as describe in [this github tutorial](https://github.com/vladimirvivien/go-cshared-examples).

To compile TSL .so libray simply run

```sh
make so
```

In the spec folder, you can find a JAVA example of the [.so TSL library](./spec/jna.md).

## License

TSL is released under a [3-BSD clause license](./LICENSE).

## Get in touch

Gitter: [gitter.im/ovh/metrics-TSL](https://gitter.im/ovh/metrics-TSL)

Twitter: [@AurrelH95](https://twitter.com/AurrelH95)
