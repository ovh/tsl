// Copyright (c) 2018-2018, OVH SAS.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/ovh/tsl/middlewares"
	"github.com/ovh/tsl/proxy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports Persistent Flags, which, if defined here,
	// will be global for your application.
	RootCmd.PersistentFlags().StringP("config", "c", "", "config file (default is $HOME/.tsl.yaml)")
	RootCmd.PersistentFlags().Bool("no-backend", false, "activate no backend mode to redirect the output to standard output")
	RootCmd.PersistentFlags().BoolP("verbose", "v", false, "verbose output")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	RootCmd.Flags().StringP("listen", "l", "127.0.0.1:8080", "listen address")

	// Bind persistent / local flags from cobra to viper
	if err := viper.BindPFlags(RootCmd.PersistentFlags()); err != nil {
		log.Fatal(err)
	}

	if err := viper.BindPFlags(RootCmd.Flags()); err != nil {
		log.Fatal(err)
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	// Bind environment variables
	viper.SetEnvPrefix("tsl")
	viper.AutomaticEnv()

	// Set config search path
	viper.AddConfigPath("/etc/tsl/")
	viper.AddConfigPath("$HOME/.tsl")
	viper.AddConfigPath(".")

	// Load config
	viper.SetConfigName("config")
	if err := viper.MergeInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Panicf("Fatal error in config file: %v \n", err)
		}
	}

	// Load user defined config
	cfgFile := viper.GetString("config")
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		err := viper.ReadInConfig()
		if err != nil {
			log.Panicf("Fatal error in config file: %v \n", err)
		}
	}

	// Set TSL default host
	viper.SetDefault("tsl.default.endpoint", "http://127.0.0.1:9090")
	viper.SetDefault("tsl.warp10.authenticate", false)

	if viper.GetBool("verbose") {
		log.SetLevel(log.DebugLevel)
	}
}

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "tsl",
	Short: "A proxy that translates queries for a TSDB backend",
	Run: func(cmd *cobra.Command, args []string) {
		// Use echo router
		r := echo.New()
		addr := viper.GetString("listen")

		// Disable echo logger
		r.Logger.SetOutput(ioutil.Discard)

		// Enable echo middlewares
		r.Use(middleware.MethodOverride())
		r.Use(middleware.Secure())
		r.Use(middleware.Recover())

		// Enable custom middlewares
		r.Use(middlewares.CORS())
		r.Use(middlewares.Logger())

		r.Any("/", func(ctx echo.Context) error {
			return ctx.NoContent(http.StatusOK)
		})

		promRegistry := prometheus.NewRegistry()

		// Register handler(s) for path(s)
		tsl := proxy.NewProxyTSL(promRegistry)
		r.POST("/v0/query", tsl.Query)

		// Use of a Prometheus custon registry to record TSL metrics
		r.Any("/metrics", echo.WrapHandler(promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{})))

		// Setup http server using native server
		server := &http.Server{
			Handler: r,
			Addr:    addr,
		}

		// Start the http server in a go routine in order to
		// handle system signal
		go func() {
			log.Infof("Start tsl server on %s", server.Addr)
			if err := server.ListenAndServe(); err != nil {
				log.Fatal(err)
			}
		}()

		/// Wait for interrupt signal to gracefully shutdown the server with
		// a timeout of 5 seconds.
		quit := make(chan os.Signal, 1)

		signal.Notify(quit, os.Interrupt)

		<-quit

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("Cannot gracefully shutdown tsl server")
		}
	},
}
