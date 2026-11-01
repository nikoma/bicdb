# Oracle Human Resources on BicDB

This is the first bounded `oracle-hr-v1` schema acceptance over PostgreSQL wire.
It retains Oracle's published sample source, records every adaptation, and uses
an ordinary PostgreSQL driver.

Follow the [profile contract and instructions](../../../docs/oracle-hr-profile.md).

```sh
python3 prepare.py --check
python3 run.py 'host=127.0.0.1 port=5440 user=bicdb dbname=bicdb'
```

Run against an empty standalone database served with
`--sql-profile oracle-hr-v1`. The loader explicitly supplies the matching
PostgreSQL startup option. It creates seven tables and imports 216 rows, then
tests the view, parameters and `ADD_JOB_HISTORY` procedure. It reports all
adaptations and does not claim general Oracle Database compatibility.

The upstream files retain their separate MIT license in `upstream/LICENSE.txt`.
The generated migration is reproducible from the pinned files with `prepare.py`.
