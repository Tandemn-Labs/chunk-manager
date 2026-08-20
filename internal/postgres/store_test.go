package postgres

import (
	"errors"
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestNormalizeDatabaseErrorClassifiesOperationalFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{
			name: "read only transaction",
			err:  &pgconn.PgError{Code: "25006", Message: "read-only SQL transaction"},
			want: ErrUnavailable,
		},
		{
			name: "deadlock",
			err:  &pgconn.PgError{Code: "40P01", Message: "deadlock detected"},
			want: ErrAborted,
		},
		{
			name: "connection exception",
			err:  &pgconn.PgError{Code: "08006", Message: "connection failure"},
			want: ErrUnavailable,
		},
		{
			name: "end of stream",
			err:  io.EOF,
			want: ErrUnavailable,
		},
		{
			name: "partial stream",
			err:  io.ErrUnexpectedEOF,
			want: ErrUnavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := normalizeDatabaseError(test.err); !errors.Is(got, test.want) {
				t.Fatalf("normalizeDatabaseError(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}
