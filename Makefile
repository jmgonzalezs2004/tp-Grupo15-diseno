SHELL := /bin/bash

up:
	mkdir -p ./output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

test:
	mkdir -p output
	rm -rf ./output/*
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans -d
	python3 -m tests.main
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: test

switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, una sola réplica de cada elemento"
	@echo "2) Un cliente, con réplica escaladas"
	@echo "3) Múltiples clientes, con réplica escaladas" 
	@read -p "Selecciona uno [1-3]: " option;	\
	cp ./scenarios/$${option}.yaml docker-compose.yaml
.PHONY: switch
