"""Independent Gregorian ADD_MONTHS values and stored-function composition."""
import datetime
import re

from compare import canonical
from routine_namespaces import identifier


def fixture(schema, prefix):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('invalid fixture prefix')
    name = f'{identifier(schema)}.{identifier(prefix + "_MONTHS")}'
    setup = [dict(backend=backend, kind='FUNCTION', name=name,
                  sql=f'CREATE FUNCTION {name}(p_date DATE,p_months NUMBER) RETURN DATE IS BEGIN RETURN ADD_MONTHS(p_date,p_months); END;',
                  inserts=[]) for backend in (0, 1)]
    values = [
        ('2024-01-31 12:34:56.999999', '1', '2024-02-29 12:34:56'),
        ('2024-02-29 12:34:56', '1', '2024-03-31 12:34:56'),
        ('2024-02-28 12:34:56', '1', '2024-03-28 12:34:56'),
        ('2023-02-28 12:34:56', '1', '2023-03-31 12:34:56'),
        ('2024-03-31 12:34:56', '-1', '2024-02-29 12:34:56'),
        ('2024-01-15 12:34:56', '1.9', '2024-02-15 12:34:56'),
        ('2024-03-15 12:34:56', '-1.9', '2024-02-15 12:34:56'),
        ('2024-02-29 00:00:00', '0', '2024-02-29 00:00:00'),
        ('2024-02-29 00:00:00', '-12', '2023-02-28 00:00:00'),
        ('1900-02-28 00:00:00', '1', '1900-03-31 00:00:00'),
        ('2000-02-29 00:00:00', '1', '2000-03-31 00:00:00'),
        ('2024-01-15 00:00:00', "'1.9'", '2024-02-15 00:00:00'),
        ('2024-01-15 00:00:00', '-0.9', '2024-01-15 00:00:00'),
        ('2024-01-15 00:00:00', '1.999999999999999999999', '2024-02-15 00:00:00'),
        ('2024-01-15 00:00:00', '-0.999999999999999999999', '2024-01-15 00:00:00'),
        ('2024-01-15 00:00:00', 'NULL', None),
        (None, '1', None), (None, 'NULL', None),
    ]
    cases = []
    for index, (stamp, months, value) in enumerate(values):
        argument = 'NULL' if stamp is None else f"TIMESTAMP '{stamp}'"
        expected = canonical(None if value is None else datetime.datetime.fromisoformat(value))
        for label, function in [('scalar', 'ADD_MONTHS'), ('routine', name)]:
            cases.append(dict(id=f'{label}_{index}', sql=f'SELECT {function}({argument},{months}) FROM DUAL',
                              expected=[[expected]]))
    return dict(setup=setup, cases=cases, compiler_probe=(prefix + '_MONTHS').lower(),
                scope='Gregorian ADD_MONTHS scalar and routine values; not alternate NLS calendars, timezone, BC boundary or full Oracle error conformance')
