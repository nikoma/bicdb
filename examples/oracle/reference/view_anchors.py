"""Reuse scalar anchor contracts through named view column descriptors."""
from routine_anchors import fixture as table_fixture
from routine_namespaces import identifier


def fixture(schema, prefix):
    plan = table_fixture(schema, prefix)
    table = identifier(prefix + "_ANCHORS")
    view = identifier(prefix + "_ANCHORS_V")
    qualified = identifier(schema) + "." + table
    setup = []
    for item in plan["setup"]:
        item = dict(item)
        if item["kind"] == "FUNCTION":
            item["sql"] = item["sql"].replace(table + ".", view + ".")
        setup.append(item)
        if item["kind"] == "TABLE":
            name = identifier(schema) + "." + view
            setup.append(dict(backend=item["backend"], kind="VIEW", name=name,
                              sql=f'CREATE VIEW {name} AS SELECT "Amount","Short.Text","Long.Amount" FROM {qualified}',
                              inserts=[]))
    plan["setup"] = setup
    plan["scope"] = "View-column scalar %TYPE descriptors, quoted names, local constraints and unconstrained formal sizes; dictionary views, privilege qualification and DDL invalidation are separate work"
    return plan
