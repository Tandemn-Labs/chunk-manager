package migrations

import "embed"

// Files contains the versioned SQL migrations used by the migration command.
//
//go:embed *.sql
var Files embed.FS
