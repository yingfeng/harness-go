.PHONY: build test clean fmt lint vet coverage examples lib bin help

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
SRC_DIR       = ./
EXAMPLES_DIR  = ./examples
BIN_DIR       = ./bin
WORKFLOW      = examples/workflow

# Build flags
LDFLAGS       = -ldflags "-s -w"
BUILD_FLAGS   = -v

## Build: compile all packages
build:
	$(GOBUILD) $(BUILD_FLAGS) ./...

## Library: compile the library as a static archive
lib:
	@echo "Building library..."
	$(GOBUILDLIB) -o $(BIN_DIR)/liblanggraph.a $(SRC_DIR)

## Test: run all tests
test:
	$(GOTEST) -v -count=1 -timeout 60s ./...

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

## Format code
fmt:
	$(GOFMT) ./...

## Run go vet
vet:
	$(GOVET) ./...

## Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BIN_DIR)

## Install dependencies
deps:
	$(GOMOD) download
	$(GOMOD) tidy

## Run linter (requires golangci-lint)
lint:
	golangci-lint run ./...

## Run benchmarks
bench:
	$(GOTEST) -bench=. -benchmem ./...

## Generate documentation
docs:
	$(GOCMD) doc -all > DOCUMENTATION.txt

## Check for security vulnerabilities (requires govulncheck)
security:
	govulncheck ./...

## All checks
check: fmt vet test

## Build and test everything
all: deps clean build test examples
	@echo "All done!"

## Production targets (ADK/CI)
.PHONY: bench-vet lint-all

bench-vet: ## Run benchmarks for agentcore
	go test ./agentcore -bench=. -benchtime=100x -run=^$

lint-all: ## Run all checks (vet + test)
	go vet ./...
	go test ./... -count=1 -timeout 180s
	@echo "All checks passed"

## Help
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
