ENVTEST_K8S_VERSION ?= 1.32.0
CONTROLLER_TOOLS_VERSION ?= v0.17.2
SETUP_ENVTEST_VERSION ?= release-0.20
IMG ?= fleetwave:dev

LOCALBIN ?= $(CURDIR)/bin
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
SETUP_ENVTEST ?= $(LOCALBIN)/setup-envtest
ENVTEST_ASSETS = $(shell $(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path 2>/dev/null)

.PHONY: all
all: verify test build

##@ Development

.PHONY: generate
generate: $(CONTROLLER_GEN) ## Regenerate deepcopy code, CRDs and RBAC from Go markers.
	$(CONTROLLER_GEN) object:headerFile=hack/boilerplate.go.txt paths=./api/...
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=fleetwave-manager paths=./... output:rbac:artifacts:config=config/rbac

.PHONY: fmt
fmt: ## gofmt the tree.
	gofmt -w .

.PHONY: vet
vet: ## go vet.
	go vet ./...

.PHONY: verify
verify: generate ## Fail if generated files or formatting are out of date.
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)
	go vet ./...
	git diff --exit-code -- api config || (echo "run 'make generate' and commit the result"; exit 1)

.PHONY: test-unit
test-unit: ## Planner, health and revision tests (no API servers).
	go test -race ./internal/planner/... ./internal/health/... ./internal/revision/...

.PHONY: test
test: $(SETUP_ENVTEST) ## All tests, including the multi-cluster envtest suite.
	KUBEBUILDER_ASSETS="$(ENVTEST_ASSETS)" go test -race -count=1 -coverprofile=cover.out ./...

.PHONY: demo
demo: $(SETUP_ENVTEST) ## Run a good, a bad and a fixed release across a simulated 4-cluster fleet.
	KUBEBUILDER_ASSETS="$(ENVTEST_ASSETS)" go run ./hack/demo

##@ Build

.PHONY: build
build: ## Build the manager binary.
	go build -o $(LOCALBIN)/fleetwave ./cmd

.PHONY: run
run: ## Run the manager against the current kubeconfig context.
	go run ./cmd --poll-interval=5s

.PHONY: docker-build
docker-build: ## Build the container image.
	docker build -t $(IMG) .

.PHONY: install
install: ## Install CRDs into the current cluster.
	kubectl apply -k config/crd

.PHONY: deploy
deploy: ## Deploy CRDs, RBAC and the manager to the current cluster.
	kubectl apply -k config/default

##@ Tools

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

$(CONTROLLER_GEN): | $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

$(SETUP_ENVTEST): | $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(SETUP_ENVTEST_VERSION)

.PHONY: help
help: ## Show targets.
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ { printf "  %-14s %s\n", $$1, $$2 } /^##@/ { printf "\n%s\n", substr($$0, 5) }' $(MAKEFILE_LIST)
