# Build
FROM golang:1.26.10 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /fleetwave ./cmd

# Run: no shell, no package manager, non-root
FROM gcr.io/distroless/static:nonroot
COPY --from=build /fleetwave /fleetwave
USER 65532:65532
ENTRYPOINT ["/fleetwave"]
