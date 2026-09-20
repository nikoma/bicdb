#!/usr/bin/env python3
"""Extra application-level assertions using only Cognee v1.4.0 public adapters."""

from __future__ import annotations

import argparse
import asyncio
import json
import os


def database_url() -> str:
    return (
        "postgresql+asyncpg://"
        f"{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}@"
        f"{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )


class EntityPoint:
    def __init__(self, identifier: str, name: str, kind: str = "Entity") -> None:
        self._data = {"id": identifier, "name": name, "type": kind}

    def model_dump(self) -> dict[str, str]:
        return dict(self._data)


async def live_contract() -> None:
    from cognee.infrastructure.databases.cache.sql.SqlCacheAdapter import SqlCacheAdapter
    from cognee.infrastructure.databases.graph.postgres.adapter import PostgresAdapter

    url = database_url()
    graph = PostgresAdapter(connection_string=url)
    await graph.initialize()
    await graph.delete_graph()
    await graph.add_nodes(
        [
            EntityPoint("primary-a", "Alpha"),
            EntityPoint("primary-b", "Beta"),
            EntityPoint("shared", "Shared", "Neighbor"),
            EntityPoint("only-a", "Only A", "Neighbor"),
            EntityPoint("only-b", "Only B", "Neighbor"),
        ]
    )
    await graph.add_edges(
        [
            ("primary-a", "shared", "R", {}),
            ("primary-b", "shared", "R", {}),
            ("primary-a", "only-a", "R", {}),
            ("primary-b", "only-b", "R", {}),
        ]
    )

    class Entity:
        pass

    or_nodes, _ = await graph.get_nodeset_subgraph(Entity, ["Alpha", "Beta"], "OR")
    and_nodes, _ = await graph.get_nodeset_subgraph(Entity, ["Alpha", "Beta"], "AND")
    assert {node[0] for node in or_nodes} == {
        "primary-a",
        "primary-b",
        "shared",
        "only-a",
        "only-b",
    }
    assert {node[0] for node in and_nodes} == {"primary-a", "primary-b", "shared"}
    await graph.close()

    cache = SqlCacheAdapter(
        url,
        lock_key="dt1262-lock",
        session_ttl_seconds=60,
        agentic_lock_timeout=1,
        purge_interval_seconds=0,
    )
    contender = SqlCacheAdapter(
        url,
        lock_key="dt1262-lock",
        agentic_lock_timeout=1,
        purge_interval_seconds=0,
    )
    await cache.prune()
    await cache.create_qa_entry(
        "dt1262-user",
        "dt1262-session",
        "question",
        "context",
        "answer",
        qa_id="dt1262-qa",
    )
    entries = await cache.get_all_qa_entries("dt1262-user", "dt1262-session")
    assert [entry.qa_id for entry in entries] == ["dt1262-qa"]
    await cache.set_value("dt1262-expiring", "value", ttl=1)
    assert await cache.get_value("dt1262-expiring") == "value"
    await asyncio.sleep(1.1)
    assert await cache.get_value("dt1262-expiring") is None

    first = await asyncio.to_thread(cache.acquire_lock)
    try:
        try:
            await asyncio.to_thread(contender.acquire_lock)
        except RuntimeError:
            pass
        else:
            raise AssertionError("contending Cognee advisory lock unexpectedly succeeded")
    finally:
        await asyncio.to_thread(cache.release_lock, first)
    second = await asyncio.to_thread(contender.acquire_lock)
    await asyncio.to_thread(contender.release_lock, second)
    await cache.close()
    await contender.close()


async def restart_contract() -> None:
    import asyncpg

    connection = await asyncpg.connect(
        user=os.environ["DB_USERNAME"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=int(os.environ["DB_PORT"]),
        database=os.environ["DB_NAME"],
    )
    try:
        tables = {
            row["table_name"]
            for row in await connection.fetch(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' "
                "AND table_name = ANY($1::text[])",
                ["graph_node", "graph_edge", "cache_qa_entries", "alembic_version"],
            )
        }
        assert tables == {"graph_node", "graph_edge", "cache_qa_entries", "alembic_version"}
        columns = {
            (row["column_name"], row["udt_name"])
            for row in await connection.fetch(
                "SELECT column_name, udt_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'graph_node'"
            )
        }
        assert ("source_ref_keys", "_varchar") in columns
        assert ("properties", "jsonb") in columns
        payload = await connection.fetchval(
            "SELECT payload FROM cache_qa_entries WHERE qa_id = $1", "dt1262-qa"
        )
        if isinstance(payload, str):
            payload = json.loads(payload)
        assert payload["answer"] == "answer"
        assert await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'vector')"
        )
    finally:
        await connection.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("live", "restart"))
    args = parser.parse_args()
    asyncio.run(live_contract() if args.mode == "live" else restart_contract())


if __name__ == "__main__":
    main()
