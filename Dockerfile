# syntax=docker/dockerfile:1

FROM golang:1.25.7-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/chunk-manager \
    ./cmd/chunk-manager

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/chunk-manager /usr/local/bin/chunk-manager

EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/chunk-manager"]
