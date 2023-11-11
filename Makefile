RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
$(eval $(RUN_ARGS):;@:)

clean:
	@rm -rf dist target


build: clean
	@poetry update
	@poetry build


package: build
	@poetry run pip install --upgrade -t target dist/*.whl


test: package
	@poetry run python run_tests.py


integration-test: package
	@poetry run python integration_test/LoadDataDynamoDB.py


install: test
	@poetry install


export-requirements:
	@poetry export --without-hashes --format=requirements.txt > requirements.txt


env-local-dev-stop:
	docker-compose -f environment/local-development/docker/docker-compose.yml down &


env-local-dev-start: env-local-dev-stop
	docker-compose -f environment/local-development/docker/docker-compose.yml up --build &


env-local-dev-terraform-init:
	terraform -chdir=environment/local-development/terraform/ init


env-local-dev-terraform-plan: package env-local-dev-terraform-init
	terraform -chdir=environment/local-development/terraform/ plan


env-local-dev-terraform-apply: env-local-dev-terraform-plan
	terraform -chdir=environment/local-development/terraform/ apply -auto-approve