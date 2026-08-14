-- name: DatabaseTime :one
SELECT clock_timestamp()::timestamptz;
