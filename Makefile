info_color := \033[0;32m
no_color := \033[0m

repo_linting := .linting
golangci_lint_ver := 2.9.0

.DEFAULT_GOAL := lint

${repo_linting}:
	mkdir -p ${repo_linting}

install-golangci-lint: ${repo_linting}
	@echo -e "$(info_color)==> $@ $(no_color)"
	@if [ -f "${repo_linting}/golangci-lint" ] && "${repo_linting}/golangci-lint" --version >/dev/null 2>&1; then \
		echo "golangci-lint is already installed"; \
	else \
		echo "golangci-lint not found or not executable, installing v${golangci_lint_ver}..."; \
		rm -f "${repo_linting}/golangci-lint"; \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b ${repo_linting} v${golangci_lint_ver} || { echo "Failed to install golangci-lint"; exit 1; }; \
		echo "Installed golangci-lint, version: $$("${repo_linting}/golangci-lint" --version 2>&1)" || { echo "Failed to verify golangci-lint version"; exit 1; }; \
	fi

deps: install-golangci-lint
	@echo -e "$(info_color)==> $@ $(no_color)"

lint: deps
	@${repo_linting}/golangci-lint config path
	@${repo_linting}/golangci-lint config verify
	@${repo_linting}/golangci-lint run --config ${PWD}/.golangci.yaml

test:
	@echo -e "$(info_color)==> $@ $(no_color)"
	@go test ./... -race -count=1 -v

test-local: 
	@go test -failfast -count=1 ./... -json -cover -race | tparse -smallscreen

# for when golangci lint does not work
# 	@fieldalignment ./...
# 	@gocritic check ./...
# 	@nilerr ./...
# 	@staticcheck ./...
# 	@unconvert -v ./...
# 	@ineffassign ./...
# 	@gocyclo -top 10 app
# 	@gocyclo -avg .
# 	@whitespace ./...