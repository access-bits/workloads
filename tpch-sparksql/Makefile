IMAGE           := akrishnaams/access-bits-tpch-sparksql:latest
DOCKERFILE      := docker/Dockerfile
COMPOSE_FILE    := docker/docker-compose.yml
COMPOSE_GENERATE := docker/docker-compose-generate.yml
ENV_FILE        := docker/.env
CONTAINER       := tpch

.PHONY: build push pull generate compose-up compose-down compose-shell run

build:
	docker build -f $(DOCKERFILE) -t $(IMAGE) .

push:
	docker push $(IMAGE)

pull:
	docker pull $(IMAGE)

# One-shot: generate TPC-H .tbl data files into ./data (SF default: 8)
generate:
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_GENERATE) up --abort-on-container-exit
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_GENERATE) down

compose-up:
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) up -d

compose-down:
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) down

compose-shell:
	docker exec -it $(CONTAINER) /bin/bash

# Run benchmark inside the already-running container
run:
	docker exec $(CONTAINER) /scripts/run.sh
