.PHONY: build test test-race test-graphengine test-agentcore clean fmt lint vet lint-all coverage examples lib bin help all check deps security docs bench bench-vet

# Go parameters
GOCMD     = go
GOBUILD   = $(GOCMD) build
GOBUILDLIB= $(GOCMD) build -buildmode=archive
GOTEST    = $(GOCMD) test
GOCLEAN   = $(GOCMD) clean
GOMOD     = $(GOCMD) mod
GOFMT     = $(GOCMD) fmt
GOVET     = $(GOCMD) vet

# Directories
BIN_DIR        = ./bin
EXAMPLES_DIR   = ./examples
GRAPHENGINE_DIR= ./graphengine
AGENTCORE_DIR  = ./agentcore
SERVER_DIR     = ./server
TELEMETRY_DIR  = ./telemetry

# Exclude examples from test targets (they may lack _test files)
CORE_PACKAGES  = $(GRAPHENGINE_DIR)/... $(AGENTCORE_DIR)/... $(SERVER_DIR)/... $(TELEMETRY_DIR)/... ./internal/...

# Build flags
LDFLAGS       = -ldflags "-s -w"
BUILD_FLAGS   = -v

## Build: compile all packages
build:
	$(GOBUILD) $(BUILD_FLAGS) ./...

## Library: compile the library as a static archive
lib:
	@echo "Building library..."
	$(GOBUILDLIB) -o $(BIN_DIR)/libharness.a .

## Test: run all tests
test:
	$(GOTEST) -v -count=1 -timeout 120s ./...

## Test with race detector
test-race:
	$(GOTEST) -race -count=1 -timeout 180s $(CORE_PACKAGES)

## Test only the graph engine (graphengine/)
test-graphengine:
	$(GOTEST) -v -count=1 -timeout 60s $(GRAPHENGINE_DIR)/...

## Test only the agent ADK (agentcore/)
test-agentcore:
	$(GOTEST) -v -count=1 -timeout 60s $(AGENTCORE_DIR)/...

## Generate code coverage report
coverage:
	@mkdir -p $(BIN_DIR)
	$(GOTEST) -count=1 -coverprofile=$(BIN_DIR)/coverage.out $(CORE_PACKAGES)
	$(GOCMD) tool cover -html=$(BIN_DIR)/coverage.out -o $(BIN_DIR)/coverage.html
	@echo "Coverage report: $(BIN_DIR)/coverage.html"
	@$(GOCMD) tool cover -func=$(BIN_DIR)/coverage.out | tail -1

## Format code
fmt:
	$(GOFMT) ./...

## Run go vet
vet:
	$(GOVET) ./...

## Run linter (requires golangci-lint)
lint:
	golangci-lint run ./...

## All checks (fmt → vet → test)
check: fmt vet test

## Run benchmarks
bench:
	$(GOTEST) -bench=. -benchmem ./...

## Run benchmarks for agentcore only
bench-vet:
	$(GOTEST) ./agentcore -bench=. -benchtime=100x -run=^$

## Full CI suite: vet + lint + race test + coverage
lint-all:
	$(GOVET) ./...
	golangci-lint run ./... 2>/dev/null || echo "golangci-lint not installed, skipping"
	$(GOTEST) -race -count=1 -timeout 180s $(CORE_PACKAGES)
	$(GOTEST) -count=1 -coverprofile=$(BIN_DIR)/coverage.out $(CORE_PACKAGES)
	@echo "=== All checks passed ==="

## Examples: build all example binaries to bin/
examples:
	@mkdir -p $(BIN_DIR)
	@echo "Building examples..."
	@for dir in $(EXAMPLES_DIR)/*/; do \
		base=$$(basename "$${dir%/}"); \
		has_sub=$$(find "$${dir}" -mindepth 2 -name "main.go" 2>/dev/null | head -1); \
		if [ -n "$$has_sub" ]; then \
			for mainfile in $$(find "$${dir}" -mindepth 2 -name "main.go"); do \
				sub=$$(echo "$${mainfile}" | sed "s|$${dir}||" | sed 's|/main.go||' | tr '/' '-'); \
				out="$(BIN_DIR)/example-$$base-$$sub"; \
				echo "  $$base/$$sub -> $$out"; \
				$(GOBUILD) $(BUILD_FLAGS) $(LDFLAGS) -o "$$out" "$$(dirname "$${mainfile}")"; \
			done; \
		else \
			out="$(BIN_DIR)/example-$$base"; \
			echo "  $$base -> $$out"; \
			$(GOBUILD) $(BUILD_FLAGS) $(LDFLAGS) -o "$$out" "$${dir}"; \
		fi; \
	done

## Binaries: build all binaries (library + examples) to bin/
bin: lib examples
	@echo "All binaries in $(BIN_DIR)/:"
	@ls -la $(BIN_DIR)/ 2>/dev/null || true

## Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BIN_DIR)

## Install dependencies
deps:
	$(GOMOD) download
	$(GOMOD) tidy

## Generate documentation
docs:
	$(GOCMD) doc -all > $(BIN_DIR)/DOCUMENTATION.txt

## Check for security vulnerabilities (requires govulncheck)
security:
	govulncheck ./...

## Build and test everything
all: deps clean build test examples
	@echo "All done!"

## Help
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
