.PHONY: test test-long bench

test:
	go vet ./...
	go test ./...

test-long:
	RAFT_SEEDS=1000 go test -run Randomized ./raft
	LIN_SECONDS=20 go test -timeout 10m -run Linearizable ./server

bench:
	go run ./cmd/bench -duration 8s -failovers 7
