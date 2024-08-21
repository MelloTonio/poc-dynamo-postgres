.PHONY: all localstack configure-aws create-table clean migrate

localstack:
	@echo "Starting LocalStack and PostgreSQL..."
	@docker-compose up -d

configure-aws:
	@echo "Configuring AWS CLI..."
	@aws configure set aws_access_key_id test
	@aws configure set aws_secret_access_key test
	@aws configure set default.region us-east-1

create-table:
	@echo "Creating DynamoDB table..."
	@aws --endpoint-url=http://localhost:4566 dynamodb create-table --table-name TestTable \
		--attribute-definitions AttributeName=Id,AttributeType=S \
		--key-schema AttributeName=Id,KeyType=HASH \
		--provisioned-throughput ReadCapacityUnits=10,WriteCapacityUnits=10
	@echo "Creating DynamoDB LeaderboardTable..."
	@aws --endpoint-url=http://localhost:4566 dynamodb create-table --table-name LeaderboardTable \
		--attribute-definitions \
			AttributeName=UserId,AttributeType=S \
			AttributeName=Timestamp,AttributeType=N \
		--key-schema \
			AttributeName=UserId,KeyType=HASH \
			AttributeName=Timestamp,KeyType=RANGE \
		--provisioned-throughput ReadCapacityUnits=1000,WriteCapacityUnits=1000
	

migrate:
	@echo "Running PostgreSQL migrations..."
	@docker exec -i $(shell docker-compose ps -q postgres) psql -U youruser -d yourdb < create_table.sql

all: localstack configure-aws create-table migrate
	@echo "LocalStack and PostgreSQL started, AWS CLI configured, and DynamoDB table created, PostgreSQL migrated."

clean:
	@echo "Stopping LocalStack and PostgreSQL..."
	@docker-compose down
