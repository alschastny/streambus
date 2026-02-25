PHPUNIT   := vendor/bin/phpunit
PSALM     := vendor/bin/psalm.phar
CS_FIXER  := vendor/bin/php-cs-fixer

.PHONY: up down psalm style style-fix \
        test test-benchmark test-parallel test-matrix \
        test-7.2 test-7.4 test-8.0 test-8.2 test-8.4 test-8.6

up:
	docker compose up -d

down:
	docker compose down

psalm:
	$(PSALM) --config=psalm.xml --no-cache

style:
	$(CS_FIXER) fix --diff --dry-run

style-fix:
	$(CS_FIXER) fix

# Run tests against Redis 8.6 with coverage (RESP2) then without (RESP3)
test:
	REDIS_HOST=127.0.0.1 REDIS_PORT=6386 REDIS_RESP_VERSION=2 XDEBUG_MODE=coverage \
		$(PHPUNIT) --testsuite main --coverage-html ./html-coverage
	REDIS_HOST=127.0.0.1 REDIS_PORT=6386 REDIS_RESP_VERSION=3 \
		$(PHPUNIT) --testsuite main

test-benchmark:
	REDIS_HOST=127.0.0.1 REDIS_PORT=6386 $(PHPUNIT) --testsuite benchmark

test-parallel:
	REDIS_HOST=127.0.0.1 REDIS_PORT=6386 $(PHPUNIT) --testsuite parallel

test-matrix: test-7.2 test-7.4 test-8.0 test-8.2 test-8.4 test-8.6

# Per-version targets — run RESP2 then RESP3 against the matching container port
define test_redis
	@printf '\n\033[1;33m=== Redis $(1) (port $(2)) ===\033[0m\n\n'
	REDIS_HOST=127.0.0.1 REDIS_PORT=$(2) REDIS_RESP_VERSION=2 $(PHPUNIT) --testsuite main
	REDIS_HOST=127.0.0.1 REDIS_PORT=$(2) REDIS_RESP_VERSION=3 $(PHPUNIT) --testsuite main
endef

test-7.2:
	$(call test_redis,7.2,6372)

test-7.4:
	$(call test_redis,7.4,6374)

test-8.0:
	$(call test_redis,8.0,6380)

test-8.2:
	$(call test_redis,8.2,6382)

test-8.4:
	$(call test_redis,8.4,6384)

test-8.6:
	$(call test_redis,8.6,6386)
