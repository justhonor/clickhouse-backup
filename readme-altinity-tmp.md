
# API:
In general, the API attempts to mirror the CLI commands.

> **GET /backup/tables**

Print list of tables: `curl -s localhost:7171/backup/tables | jq .`

> **POST /backup/create**

Create new backup: `curl -s localhost:7171/backup/create -X POST | jq .`
* Optional query argument `table` works the same as the `--table value` CLI argument.
* Optional query argument `freeze_one_by_one` works the same the `--freeze-one-by-one` CLI argument.
* Optional query argument `name` works the same as specifying a backup name with the CLI.
* Full example: `curl -s 'localhost:7171/backup/create?table=default.billing&name=billing_test&freeze_one_by_one' -X POST`

> **POST /backup/upload**

Upload backup to remote storage: `curl -s localhost:7171/backup/upload/<BACKUP_NAME> -X POST | jq .`
* Optional query argument `diff-from` works the same as the `--diff-from` CLI argument.

> **GET /backup/list**

Print list of backups: `curl -s localhost:7171/backup/list | jq .`

> **POST /backup/download**

Download backup from remote storage: `curl -s localhost:7171/backup/download/<BACKUP_NAME> -X POST | jq .`

> **POST /backup/restore**

Create schema and restore data from backup: `curl -s localhost:7171/backup/restore/<BACKUP_NAME> -X POST | jq .`
* Optional query argument `table` works the same as the `--table value` CLI argument.
* Optional query argument `schema` works the same the `--schema` CLI argument (restore schema only).
* Optional query argument `data` works the same the `--data` CLI argument (restore data only).

> **POST /backup/delete**

Delete specific backup: `curl -s localhost:7171/backup/delete/<BACKUP_NAME> -X POST | jq .`

> **POST /backup/freeze**

Freeze tables: `curl -s localhost:7171/backup/freeze -X POST | jq .`

> **POST /backup/clean**

Remove data in 'shadow' folder: `curl -s localhost:7171/backup/clean -X POST | jq .`

# TODOS:
* diff-from (?)
* prometheus metrics
