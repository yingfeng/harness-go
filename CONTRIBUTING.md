# Contributing to Agent Harness Go

Thank you for your interest in contributing to Agent Harness Go! This document provides guidelines for contributing to the project.

## Code of Conduct

This project and everyone participating in it is governed by our Code of Conduct. By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues to see if the problem has already been reported. When you are creating a bug report, please include as many details as possible:

- Use a clear and descriptive title
- Describe the exact steps to reproduce the problem
- Provide specific examples to demonstrate the steps
- Describe the behavior you observed and what behavior you expected
- Include code samples and error messages

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please include:

- A clear and descriptive title
- A detailed description of the proposed enhancement
- Examples of how the enhancement would be used
- Explain why this enhancement would be useful

### Pull Requests

1. Fork the repository
2. Create a new branch from `main` for your feature or bug fix
3. Make your changes
4. Add or update tests as necessary
5. Ensure all tests pass
6. Update documentation if needed
7. Submit a pull request

## Development Setup

### Prerequisites

- Go 1.21 or later
- Make (optional, for using Makefile commands)

### Setup

```bash
# Clone your fork
git clone https://github.com/yourusername/harness-go.git
cd harness-go

# Install dependencies
go mod download

# Run tests
go test ./...
```

### Project Structure

```
harness-go/
├── channels/      # Channel implementations
├── checkpoint/    # Checkpoint savers
├── constants/     # Constants
├── errors/        # Error types
├── examples/      # Example applications
├── graph/         # Graph building and execution
├── interrupt/     # Human-in-the-loop functionality
├── types/         # Core types
└── utils/         # Utilities
```

## Style Guidelines

### Go Code Style

We follow the standard Go style guidelines:

- Use `gofmt` to format your code
- Follow the [Effective Go](https://golang.org/doc/effective_go.html) guidelines
- Use meaningful variable and function names
- Add comments for exported functions, types, and packages
- Keep functions focused and reasonably sized

### Testing

- Write unit tests for new functionality
- Aim for good test coverage
- Use table-driven tests where appropriate
- Test error cases as well as success cases

Example test:

```go
func TestMyFunction(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected string
        wantErr  bool
    }{
        {
            name:     "valid input",
            input:    "hello",
            expected: "HELLO",
            wantErr:  false,
        },
        {
            name:    "empty input",
            input:   "",
            wantErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := MyFunction(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("MyFunction() error = %v, wantErr %v", err, tt.wantErr)
                return
            }
            if result != tt.expected {
                t.Errorf("MyFunction() = %v, expected %v", result, tt.expected)
            }
        })
    }
}
```

### Documentation

- Add package documentation comments
- Document exported functions, types, and methods
- Provide examples where helpful
- Update README.md if adding significant features

### Commit Messages

Use clear and meaningful commit messages:

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or less
- Reference issues and pull requests liberally after the first line

## Testing Your Changes

Before submitting a pull request:

```bash
# Format code
make fmt

# Run linter
make lint

# Run tests
make test

# Run tests with coverage
make coverage

# Build examples
make examples
```

## Review Process

Pull requests will be reviewed by maintainers. The review process includes:

1. Automated checks (tests, linting, formatting)
2. Code review by maintainers
3. Discussion and feedback
4. Approval and merge

Please be patient during the review process. We aim to respond to pull requests within a few days.

## License

By contributing to Agent Harness Go, you agree that your contributions will be licensed under the MIT License.

## Questions?

If you have questions or need help, please:

- Open an issue for discussion
- Check existing documentation
- Review existing code for examples

Thank you for contributing to Agent Harness Go!
