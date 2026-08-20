# Python lifecycle example

Run these commands from the repository root. Create a virtual environment and
generate the Python protobuf modules outside the repository:

```sh
python3 -m venv /tmp/chunk-manager-python-venv
source /tmp/chunk-manager-python-venv/bin/activate
python -m pip install -r examples/python/requirements.txt

STUB_DIR="$(mktemp -d)"
python -m grpc_tools.protoc \
  -I proto \
  --python_out="$STUB_DIR" \
  --grpc_python_out="$STUB_DIR" \
  proto/tandemn/chunkmanager/v1/chunk_manager.proto
export PYTHONPATH="$STUB_DIR"
```

In a separate terminal, migrate a writable PostgreSQL database and start the Go
service:

```sh
export DATABASE_URL='postgresql://user@host/chunk_manager'
go run ./cmd/dbmigrate up
go run ./cmd/chunk-manager
```

Back in the Python terminal, run the lifecycle:

```sh
python examples/python/client.py
```

Use `--target host:port` to connect somewhere other than `127.0.0.1:9090`.
