// Package migrations embeds the plain-SQL schema migrations so the binary and
// its tests can run migrations without depending on the working directory.
package migrations

import "embed"

// FS holds every .sql migration at the module root migrations/ directory.
//
//go:embed *.sql
var FS embed.FS
