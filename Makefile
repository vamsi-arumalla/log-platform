.PHONY: build test lint run-local docker-build deploy clean

SERVICES := ingestor query compactor
REGISTRY := ghcr.io/vamsi-arumalla

build:
	@for svc in $(SERVICES); do \
		echo "Building $$svc..."; \
		go build -o bin/$$svc ./cmd/$$svc; \
	done

test:
	go test -race -count=1 ./...

lint:
	go vet ./...

run-local:
	docker compose up -d kafka minio prometheus grafana
	@echo "Waiting for Kafka..."
	@sleep 15
	docker compose up -d ingestor query

docker-build:
	@for svc in $(SERVICES); do \
		docker build --build-arg SERVICE=$$svc -t $(REGISTRY)/log-platform-$$svc:latest .; \
	done

deploy:
	kubectl apply -f k8s/base/namespace.yaml
	kubectl apply -f k8s/base/
	kubectl apply -f k8s/monitoring/
	kubectl apply -f k8s/autoscaling/

loadtest:
	./scripts/loadtest.sh

failover-test:
	./scripts/failover-test.sh

clean:
	rm -rf bin/
	docker compose down -v
