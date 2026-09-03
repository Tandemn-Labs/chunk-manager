export PGPASSWORD="$(
  kubectl get secret chunk-manager-app -n default \
    -o jsonpath='{.data.password}' | base64 --decode
)"

export PGSSLMODE=disable
export PSQL_PAGER=less