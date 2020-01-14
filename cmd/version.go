package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

var (
	version   = "0.0.9-preview"
	githash   = "HEAD"
	gitbranch = "master"
	date      = "1970-01-01T00:00:00Z UTC"
	rawPrint  = false
)

func init() {
	versionCmd.PersistentFlags().BoolVarP(&rawPrint, "raw", "r", false, "print only the version number")
	RootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number",
	Run: func(cmd *cobra.Command, args []string) {
		if rawPrint {
			fmt.Print(version)
		} else {
			fmt.Printf("tsl server version %s %s/%s\n", version, gitbranch, githash)
			fmt.Printf("tsl server build date %s\n", date)
			fmt.Printf("go version %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
		}
	},
}
