"""Client disk/transport recovery; server SQL behavior is covered by store tests."""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from filelock import FileLock, Timeout

from omnigent.preference_client import PreferenceClient
from omnigent.preference_sync import PreferenceMutation, SharedPreferenceKey
from omnigent.stores.preference_store.sqlalchemy_store import SqlAlchemyPreferenceStore

MODEL = SharedPreferenceKey(kind="model", name="model")


@pytest.fixture
def peers(tmp_path, db_uri):
    store = SqlAlchemyPreferenceStore(db_uri, authority_id="client-test")
    clients = [
        PreferenceClient(
            tmp_path / name / "cache.db", identity=store.identity("alice"), installation_id=name
        )
        for name in ("laptop", "remote")
    ]
    return store, clients


def transport(store, *, user="alice", after_write=None):
    def handle(request):
        if request.method == "GET":
            result = store.read(user_id=user)
        else:
            operation = PreferenceMutation.model_validate_json(request.content)
            result = store.apply(operation, user_id=user)
            if after_write is not None:
                after_write(request, result)
        return httpx.Response(200, json=result.model_dump(mode="json"))

    return httpx.MockTransport(handle)


@pytest.mark.asyncio
async def test_offline_chain_survives_reopen_and_noops(peers):
    store, (local, remote) = peers
    with pytest.raises(ValueError, match="Synchronize once"):
        local.enqueue(MODEL, "a")
    async with httpx.AsyncClient(base_url="http://test", transport=transport(store)) as http:
        await local.sync(http)
        for value in ("a", "a", None, "b"):
            local.enqueue(MODEL, value)
        assert local.cached().entries == []
        reopened = PreferenceClient(
            local.path, identity=store.identity("alice"), installation_id="laptop"
        )
        result = await reopened.sync(http, max_operations=2)
        assert len(reopened.pending()) == 2
        assert result.entries[0].version == 1
        await reopened.sync(http)
        result = await remote.sync(http)
    assert result.entries[0].value == "b" and result.entries[0].version == 3
    assert result.revision == 4 and result.conflicts == []
    assert reopened.pending() == []


@pytest.mark.asyncio
async def test_lost_ack_and_newer_remote_edit_cannot_regress_cache(peers):
    store, (local, remote) = peers
    delivered = []

    def drop(request, _result):
        delivered.append(json.loads(request.content))
        raise httpx.ReadError("lost response", request=request)

    async with httpx.AsyncClient(base_url="http://test", transport=transport(store)) as http:
        await local.sync(http)
        local.enqueue(MODEL, "a")
        async with httpx.AsyncClient(
            base_url="http://test", transport=transport(store, after_write=drop)
        ) as lossy:
            with pytest.raises(httpx.ReadError):
                await local.sync(lossy)
        await remote.sync(http)
        remote.enqueue(MODEL, "b")
        await remote.sync(http)
        local.enqueue(MODEL, "c")
        assert local.pending()[0].model_dump(mode="json") == delivered[0]
        snapshot = await local.sync(http)
    assert snapshot.entries[0].value == "b"
    assert [conflict.value for conflict in snapshot.conflicts] == ["c"]
    assert local.pending() == []


@pytest.mark.asyncio
async def test_conflicting_offline_chain_requires_explicit_resolution(peers):
    store, (local, remote) = peers
    async with httpx.AsyncClient(base_url="http://test", transport=transport(store)) as http:
        await local.sync(http)
        await remote.sync(http)
        local.enqueue(MODEL, "a")
        remote.enqueue(MODEL, "b")
        remote.enqueue(MODEL, "c")
        await local.sync(http)
        snapshot = await remote.sync(http)
        assert snapshot.entries[0].value == "a"
        assert {item.value for item in snapshot.conflicts} == {"b", "c"}
        remote.enqueue(
            MODEL, "c", resolves=tuple(item.operation_id for item in snapshot.conflicts)
        )
        await remote.sync(http)
        snapshot = await local.sync(http)
    assert snapshot.entries[0].value == "c" and snapshot.conflicts == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["offline", "forbidden", "wrong-user", "bad-receipt", "cancelled"]
)
async def test_delivery_failure_preserves_outbox(peers, failure):
    store, (local, _) = peers
    async with httpx.AsyncClient(base_url="http://test", transport=transport(store)) as http:
        await local.sync(http)
    local.enqueue(MODEL, "a")
    queued = local.pending()
    posts = []

    def handle(request):
        if failure == "offline":
            raise httpx.ConnectError("offline", request=request)
        if failure == "forbidden":
            return httpx.Response(401)
        if request.method == "GET":
            snapshot = store.read(user_id="bob" if failure == "wrong-user" else "alice")
            return httpx.Response(200, json=snapshot.model_dump(mode="json"))
        posts.append(request)
        if failure == "cancelled":
            raise asyncio.CancelledError
        return httpx.Response(200, json={"operation_id": "invalid"})

    async with httpx.AsyncClient(
        base_url="http://test", transport=httpx.MockTransport(handle)
    ) as http:
        with pytest.raises((httpx.HTTPError, ValueError, asyncio.CancelledError)):
            await local.sync(http)
    assert local.pending() == queued
    if failure == "wrong-user":
        assert posts == []


@pytest.mark.asyncio
async def test_cache_identity_corruption_and_stale_snapshot_are_not_silent_resets(peers):
    store, (local, _) = peers
    with pytest.raises(ValueError, match="another identity"):
        PreferenceClient(local.path, identity=store.identity("bob"), installation_id="laptop")
    async with httpx.AsyncClient(base_url="http://test", transport=transport(store)) as http:
        old = await local.sync(http)
        local.enqueue(MODEL, "a")
        latest = await local.sync(http)
    with pytest.raises(ValueError, match="older than"):
        local._accept_snapshot(old)
    with pytest.raises(ValueError, match="lost an acknowledged"):
        local._accept_snapshot(latest.model_copy(update={"entries": []}))
    assert local.cached() == latest
    with local._transaction() as connection:
        connection.execute("UPDATE state SET snapshot = 'corrupt'")
    with pytest.raises(ValueError):
        local.cached()


@pytest.mark.asyncio
async def test_concurrent_local_writers_and_sync_lock(peers):
    store, (local, _) = peers
    async with httpx.AsyncClient(base_url="http://test", transport=transport(store)) as http:
        await local.sync(http)
        with ThreadPoolExecutor(max_workers=4) as pool:
            ids = list(pool.map(lambda value: local.enqueue(MODEL, str(value)), range(12)))
        assert {item.operation_id for item in local.pending()} == set(ids)
        with FileLock(str(local.path) + ".sync.lock"):
            with pytest.raises(Timeout):
                await local.sync(http)
            with pytest.raises(Timeout):
                local.discard(local.pending()[0].operation_id)
        snapshot = await local.sync(http)
    assert snapshot.entries[0].version == 12 and snapshot.conflicts == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["operation_id", "key", "value"])
async def test_mismatched_receipt_does_not_acknowledge_an_edit(peers, change):
    from uuid import uuid4

    store, (local, _) = peers
    async with httpx.AsyncClient(base_url="http://test", transport=transport(store)) as http:
        await local.sync(http)
    local.enqueue(MODEL, "a")
    queued = local.pending()

    def handle(request):
        if request.method == "GET":
            result = store.read(user_id="alice")
        else:
            result = store.apply(
                PreferenceMutation.model_validate_json(request.content), user_id="alice"
            )
            if change == "operation_id":
                result = result.model_copy(update={"operation_id": uuid4()})
            else:
                entry = result.entry.model_copy(
                    update={
                        change: SharedPreferenceKey(kind="model", name="opencode_model")
                        if change == "key"
                        else "wrong"
                    }
                )
                result = result.model_copy(update={"entry": entry})
        return httpx.Response(200, json=result.model_dump(mode="json"))

    async with httpx.AsyncClient(
        base_url="http://test", transport=httpx.MockTransport(handle)
    ) as http:
        with pytest.raises(ValueError, match="receipt"):
            await local.sync(http)
    assert local.pending() == queued


@pytest.mark.asyncio
async def test_outbox_limit_and_invalid_edit_leave_existing_work(peers, monkeypatch):
    import omnigent.preference_client as module

    store, (local, _) = peers
    async with httpx.AsyncClient(base_url="http://test", transport=transport(store)) as http:
        with pytest.raises(ValueError, match="nonnegative"):
            await local.sync(http, max_operations=-1)
        await local.sync(http)
    with pytest.raises(ValueError):
        local.enqueue(MODEL, False)
    monkeypatch.setattr(module, "MAX_KEYS", 1)
    operation_id = local.enqueue(MODEL, "a")
    with pytest.raises(ValueError, match="outbox is full"):
        local.enqueue(MODEL, "b")
    assert [operation.operation_id for operation in local.pending()] == [operation_id]


@pytest.mark.asyncio
async def test_discard_uncertain_head_preserves_successor_conflicts(peers):
    store, (local, remote) = peers
    async with httpx.AsyncClient(base_url="http://test", transport=transport(store)) as http:
        await local.sync(http)
        first = local.enqueue(MODEL, "a")
        successor = local.enqueue(MODEL, "b")
        # The server committed, but the client has not received its receipt.
        store.apply(local.pending()[0], user_id="alice")
        before = local.pending()
        with pytest.raises(ValueError, match="first queued"):
            local.discard(successor)
        assert local.pending() == before
        assert local.discard(first) == before[0]
        assert local.pending() == [before[1]]
        snapshot = await local.sync(http)
        assert snapshot.entries[0].value == "a"
        assert [item.value for item in snapshot.conflicts] == ["b"]
        assert await remote.sync(http) == snapshot
        with pytest.raises(ValueError, match="first queued"):
            local.discard(first)
