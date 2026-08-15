package main

import (
	"net/http"
	"os"
)

func main() {
	http.HandleFunc("/", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	if err := http.ListenAndServe(":9090", nil); err != nil {
		_, _ = os.Stderr.WriteString("local listener unavailable\n")
		os.Exit(1)
	}
}
