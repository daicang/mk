lint:
	golangci-lint run -v

test:
	go test -cover -v ./...

test-db:
	go test -v ./pkg/db
