IMAGE := akrishnaams/access-bit-ycsb-memcached:latest
DOCKERFILE := docker/Dockerfile
YCSB_CONTAINER := ycsb
MEMCACHED_CONTAINER := memcached
CONFIG ?=

.PHONY: build push pull ycsb-run memcached-run

build:
	docker build -f $(DOCKERFILE) -t $(IMAGE) .

push:
	docker push $(IMAGE)

pull:
	docker pull $(IMAGE)
	docker pull memcached

ycsb-run:
	@docker rm -f $(YCSB_CONTAINER) 2>/dev/null || true
	docker run -d --name $(YCSB_CONTAINER) $(IMAGE) sleep infinity
	$(if $(CONFIG),docker cp $(CONFIG) $(YCSB_CONTAINER):/YCSB/$(notdir $(CONFIG)))
	docker exec -it $(YCSB_CONTAINER) /bin/bash

memcached-run:
	@if [ -z "$(CONFIG)" ]; then echo "Error: CONFIG is required. Usage: make memcached-run CONFIG=<path>"; exit 1; fi
	@docker rm -f $(MEMCACHED_CONTAINER) 2>/dev/null || true
	$(eval MEM     := $(shell python3 -c "import json; d=json.load(open('$(CONFIG)')); print(d['memory'])"))
	$(eval PORT    := $(shell python3 -c "import json; d=json.load(open('$(CONFIG)')); print(d['port'])"))
	$(eval THREADS := $(shell python3 -c "import json; d=json.load(open('$(CONFIG)')); print(d['threads'])"))
	docker run -d --name $(MEMCACHED_CONTAINER) --net host memcached:latest -m $(MEM) -p $(PORT) -t $(THREADS)