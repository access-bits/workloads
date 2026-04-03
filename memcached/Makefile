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

compose-up-memcached:
	docker compose -f $(COMPOSE_FILE) up -d memcached

compose-up-ycsb:
	docker compose -f $(COMPOSE_FILE) up -d --no-deps ycsb

compose-down-memcached:
	docker compose -f $(COMPOSE_FILE) down memcached

compose-down-ycsb:
	docker compose -f $(COMPOSE_FILE) down ycsb
	
compose-shell:
	docker exec -it $(YCSB_CONTAINER) /bin/bash