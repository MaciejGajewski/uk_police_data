.PHONY: test

help:
	@echo "run-etl - run etl module to create local parquet files from source csv"
	@echo "run-services - run kpi module to collect metrics based on parquet files"

init:
	pip install -r requirements.txt
	
run-etl:
	python main_etl.py

run-services:
	python main_services.py

test:
	py.test src/tests

clean:
	rm -rf __pycache__
	
docker-build:
	docker build -t uk_app .
	
docker-run:
	docker run -u root uk_app driver local:///opt/application/main_etl.py