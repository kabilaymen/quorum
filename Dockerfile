FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /quorum ./cmd/quorum \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /loadgen ./cmd/loadgen \
 && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /quorum /loadgen /
# named volumes copy this directory's ownership: the non-root user can write the WAL
COPY --from=build --chown=65532:65532 /data /data
EXPOSE 7000 8080
ENTRYPOINT ["/quorum"]
