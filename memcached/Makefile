IMAGE := akrishnaams/access-bit-ycsb-memcached:latest
DOCKERFILE := docker/Dockerfile
COMPOSE_FILE := docker/docker-compose.yml
YCSB_CONTAINER := ycsb
MEMCACHED_CONTAINER := memcached
CONFIG ?=

.PHONY: build push pull compose-up compose-down compose-shell

build:
	docker build -f $(DOCKERFILE) -t $(IMAGE) .

push:
	docker push $(IMAGE)

pull:
	docker pull $(IMAGE)
	docker pull memcached

compose-up:
	docker compose -f $(COMPOSE_FILE) up -d

compose-down:
	docker compose -f $(COMPOSE_FILE) down

compose-shell:
	docker exec -it $(YCSB_CONTAINER) /bin/bash