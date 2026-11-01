# Oracle reference queries

This directory compares a fixed set of public queries against an actual Oracle
database and BicDB's `oracle-hr-v1` profile. The comparison checks query values,
including nulls, character padding, exact numbers and time components. It does
not qualify type metadata, error semantics, transactions or general Oracle
compatibility. There is no allowlist of ignored differences: any difference or
execution error makes `compare.py` exit with status 1, after writing its report.

## Local reference database

Use a dedicated database and schema. The development reference uses the official
[Oracle Free Lite image](https://container-registry.oracle.com/ords/ocr/ba/database/free),
version `23.26.3.0-lite`, pinned to this multi-architecture manifest:

```text
container-registry.oracle.com/database/free@sha256:cf540c3fa190d7cffad08c491652ac07fc70314e510f6b87890449517d565e94
```

Review the image's Oracle Free Use Terms before downloading it. Oracle is an
external test dependency; its binaries, database files and credentials are not
part of BicDB's source distribution. Lite omits Data Pump, RMAN, XML DB and other
features listed in the registry documentation. Use a separate full-image
container and volume to qualify those features. Results on this newer Free
edition do not establish behavior on Oracle 19c or Enterprise Edition.

An example local setup, with Docker already installed:

```bash
install -d -m 700 "$HOME/.local/state/bicdb/oracle-reference"
python3 - <<'PY'
import os
import secrets
from pathlib import Path
path = Path.home() / '.local/state/bicdb/oracle-reference/oracle_pwd'
# The containing directory is private; the bound file must be readable by the
# container's Oracle user. Never put this directory inside the repository.
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
with os.fdopen(fd, 'w') as secret:
    secret.write('BicR7' + secrets.token_hex(12))
PY
docker volume create bicdb-oracle-reference-data
docker run -d --name bicdb-oracle-reference \
  --label org.bicdb.purpose=oracle-reference \
  --restart unless-stopped --cpus 2 --memory 4g --shm-size 1g \
  --publish 127.0.0.1:1521:1521 \
  --mount "type=bind,src=$HOME/.local/state/bicdb/oracle-reference/oracle_pwd,dst=/run/secrets/oracle_pwd,readonly" \
  --mount type=volume,src=bicdb-oracle-reference-data,dst=/opt/oracle/oradata \
  container-registry.oracle.com/database/free@sha256:cf540c3fa190d7cffad08c491652ac07fc70314e510f6b87890449517d565e94
docker logs bicdb-oracle-reference
docker port bicdb-oracle-reference 1521/tcp
```

Wait for `DATABASE IS READY TO USE!`. The named volume survives container
recreation. `docker restart bicdb-oracle-reference` restarts it without deleting
its data. Inspect the existing container and volume before reusing their names;
these commands do not replace existing containers. Choose another fixed host
port if 1521 is occupied. The port is bound only to localhost; using a fixed
port keeps client configuration valid across restarts. The PDB service is
`FREEPDB1`.

Create a dedicated HR schema using an administrative session:

```bash
docker exec -it bicdb-oracle-reference sqlplus / as sysdba
```

```sql
ALTER SESSION SET CONTAINER=FREEPDB1;
CREATE TABLESPACE bicdb_reference
  DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/bicdb_reference01.dbf'
  SIZE 64M AUTOEXTEND ON NEXT 16M MAXSIZE 256M;
SET VERIFY OFF
ACCEPT fixture_password CHAR PROMPT 'New HR fixture password: ' HIDE
CREATE USER bicdb_hr IDENTIFIED BY "&fixture_password"
  DEFAULT TABLESPACE bicdb_reference QUOTA 64M ON bicdb_reference;
UNDEFINE fixture_password
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE,
  CREATE PROCEDURE, CREATE TRIGGER TO bicdb_hr;
```

Store the fixture password in a private file outside the checkout. A private
connection JSON file should have the following shape; `port` is the actual port
reported by Docker and `password_file` is relative to the JSON file or absolute:

```json
{
  "host": "127.0.0.1",
  "port": 1521,
  "service_name": "FREEPDB1",
  "user": "BICDB_HR",
  "password_file": "hr-password"
}
```

## Import and compare

Install the two Python drivers in a virtual environment:

```bash
python3 -m venv /tmp/bicdb-oracle-reference-venv
/tmp/bicdb-oracle-reference-venv/bin/pip install 'oracledb==26.1.0' 'psycopg[binary]==3.3.6'
python3 examples/oracle/hr/prepare.py --check
/tmp/bicdb-oracle-reference-venv/bin/python examples/oracle/reference/load_hr.py \
  --oracle-config "$HOME/.local/state/bicdb/oracle-reference/hr-connection.json"
```

`load_hr.py` refuses administrative accounts and nonempty schemas. It loads the
same adapted fixture as the BicDB example, checks all seven row counts, and
rejects invalid compiled objects. Oracle DDL commits independently, so a failed
import may leave objects behind: recreate only the dedicated fixture schema
before retrying. The loader never drops objects automatically.

Load the [BicDB HR example](../../../docs/oracle-hr-profile.md) into a separate,
new BicDB database, then compare:

```bash
export BICDB_PG_DSN='host=127.0.0.1 port=5441 user=bicdb dbname=bicdb'
/tmp/bicdb-oracle-reference-venv/bin/python examples/oracle/reference/compare.py \
  --oracle-config "$HOME/.local/state/bicdb/oracle-reference/hr-connection.json" \
  --output /tmp/bicdb-oracle-comparison.json
```

The report includes versions, a fixture hash, and every comparison result. It
contains no DSNs or credentials. Its queries read only the public sample. The
history query deliberately selects the original sample period, excluding the
additional 2020 procedure-validation row created by `hr/run.py`.

Run driver-independent harness checks with:

```bash
python3 -m unittest discover -s examples/oracle/reference -p 'test_*.py'
```

The regular CI suite runs these harness checks. Live Oracle comparisons require
the external reference database and must be reported separately from CI checks.

The preview's standalone datetime extraction fixtures use
`--profile oracle-preview-v1 --cases examples/oracle/reference/extract-cases.json`.
They check date year/month/day fields, timestamp clock fields, conditional
aggregates and an application column named `YEAR`. These are value comparisons;
they do not qualify all Oracle datetime types, errors, NLS rules or metadata.
