GO_LINT=$(shell which golangci-lint 2> /dev/null || echo '')
GO_LINT_URI=github.com/golangci/golangci-lint/cmd/golangci-lint@latest
GO_SEC=$(shell which gosec 2> /dev/null || echo '')
GO_SEC_URI=github.com/securego/gosec/v2/cmd/gosec@latest
GO_VULNCHECK=$(shell which govulncheck 2> /dev/null || echo '')
GO_VULNCHECK_URI=golang.org/x/vuln/cmd/govulncheck@latest

.PHONY: lint
lint:
	$(if $(GO_LINT), ,go install $(GO_LINT_URI))
	golangci-lint run -v
.PHONY: sec
sec:
	$(if $(GO_SEC), ,go install $(GO_SEC_URI))
	gosec -exclude-generated ./...
.PHONY: vuln
vuln:
	$(if $(GO_VULNCHECK), ,go install $(GO_VULNCHECK_URI))
	govulncheck ./...
.PHONY: verify
verify: lint sec vuln
test:
	go test -v -covermode=atomic -coverpkg ./... ./... -coverprofile=coverage.coverprofile
	@grep -v -E -f .covignore coverage.coverprofile > coverage.filtered.coverprofile
	@mv coverage.filtered.coverprofile coverage.coverprofile
	go tool cover -html=coverage.coverprofile -o cover.html
	@echo total:
	@go tool cover -func coverage.coverprofile | grep -e 'total:' | grep -o '[0-9.]*' | cat