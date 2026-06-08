.PHONY: all backend frontend clean

all: backend

# Build the Go backend
backend:
	cd backend && go build -o server .

# Run the Go backend (requires MYSQL_DSN or OPENAI_API_KEY)
run-backend:
	cd backend && ./server

# Run frontend dev server
run-frontend:
	npm run dev

# Install dependencies
install:
	cd backend && go mod tidy
	npm install

# Clean build artifacts
clean:
	rm -f backend/server
	rm -rf .next
