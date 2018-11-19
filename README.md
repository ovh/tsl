# Time Series Language (TSL)

> TSL is a HTTP proxy which generate [_WarpScript_](https://www.warp10.io) or a [_PromQl script_](https://prometheus.io/docs/prometheus/latest/querying/basics/) based on a TSL query, then execute it on a _Warp 10_ or a _Prometheus_ backend. To get more information about a TSL query, you can have a look at our [initial spec](./spec/spec.md) and our [query doc](./spec/doc.md).

## Pre-install

To build and install Tsl you need:

- A working [_go_](https://golang.org) install, with the `GOROOT`and `GOPATH` variables correctly set
- `make` in order to use the `Makefile`

## Install

1. Install the tooling

   `make init`

2. Grab the dependencies

   `make dep`

3. Build TSL

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

## Licensing

See the LICENSE file.

## Get in touch

Gitter: [gitter.im/ovh/metrics-TSL](https://gitter.im/ovh/metrics-TSL)

Twitter: [@AurrelH95](https://twitter.com/AurrelH95)
