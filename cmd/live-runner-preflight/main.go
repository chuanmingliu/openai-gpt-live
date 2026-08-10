// Command live-runner-preflight fails closed unless the current host exactly
// matches the reviewed formal load profile. Its output never contains host or
// profile fields.
package main

import (
	"flag"
	"io"
	"os"

	"github.com/chuanmingliu/openai-gpt-live/internal/loadgen"
)

func main() {
	os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr))
}

func execute(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("live-runner-preflight", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	profile := flags.String("profile", "", "checked-in exact formal runner profile")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *profile == "" || loadgen.VerifyPinnedHost(*profile) != nil {
		_, _ = io.WriteString(stderr, "pinned runner preflight failed\n")
		return 1
	}
	_, _ = io.WriteString(stderr, "pinned runner preflight passed\n")
	return 0
}
