GOLANGCI_VERSION = 1.21.0

ci: lint test ##=> Run all CI targets
.PHONY: ci

bin/golangci-lint: bin/golangci-lint-${GOLANGCI_VERSION}
	@ln -sf golangci-lint-${GOLANGCI_VERSION} bin/golangci-lint
bin/golangci-lint-${GOLANGCI_VERSION}:
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | BINARY=golangci-lint bash -s -- v${GOLANGCI_VERSION}
	@mv bin/golangci-lint $@

lint: bin/golangci-lint ##=> Lint all the things
	@echo "--- lint all the things"
	@$(shell pwd)/bin/golangci-lint run
.PHONY: lint

clean: ##=> Clean all the things
	$(info [+] Clean all the things...")
.PHONY: clean

test: ##=> Run the tests
	$(info [+] Run tests...")
	@go test -v -cover ./...
.PHONY: test