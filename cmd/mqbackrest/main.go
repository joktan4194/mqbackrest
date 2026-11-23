// Command mqbackrest is a reliable, parallel backup and restore tool for MySQL.
package main

import (
	"os"

	"github.com/joktan4194/mqbackrest/internal/cli"
)

func main() { os.Exit(cli.Execute()) }
