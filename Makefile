run_mysql: ensure_db
	cargo run -p file_watcher -- mysql_test_config.toml

ensure_db:
	docker compose up -d --wait db

run_tests:
	cargo nextest run

build_file_watcher:
	docker build --output "./docker_outputs" --target export .

release_file_watcher: run_tests build_file_watcher
	gh release upload file_watcher_v1 ./docker_outputs/file_watcher --clobber 

release: release_file_watcher

all: run_tests
