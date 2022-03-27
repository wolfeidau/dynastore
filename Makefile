GOLANGCI_VERSION = 1.45.2

ci: lint test ##=> Run all CI targets
.PHONY: ci

lint:
	@echo "--- lint all the things"
	docker run --rm -v $(shell pwd):/app -w /app golangci/golangci-lint:v$(GOLANGCI_VERSION) golangci-lint run -v
.PHONY: lint

clean:
	$(info [+] Clean all the things...)
.PHONY: clean

test:
	@echo "--- test all the things"
	@go test -v -covermode=count -coverprofile=coverage.txt ./ 
.PHONY: test
