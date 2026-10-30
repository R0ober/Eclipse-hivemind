"""Hivemind 1.1.12 DHT hooks, installed only inside the new runner's daemon.

Filter remote FIND responses, without changing storage, neighbors or expiration.
Raw response observations precede DHTNode's dictionary merge. Local copies are
observed separately; concurrent/reused searches can leave provenance unknown.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import importlib.metadata
import uuid

import hivemind
from hivemind.dht.node import DHTNode
from hivemind.dht.protocol import DHTProtocol
from hivemind.dht.routing import DHTID
from hivemind.dht.storage import DictionaryDHTValue
from hivemind.p2p import PeerID, P2PContext
from hivemind.proto import dht_pb2
from hivemind.utils import get_dht_time

from .eclipse import CONTROLLED_AVERAGER_ROLES, HIVEMIND_VERSION, averager_role

query_context = contextvars.ContextVar("eclipse_dht_query", default=None)
averager_query = contextvars.ContextVar("eclipse_averager_query", default=None)
store_context = contextvars.ContextVar("eclipse_store", default=None)


def verify_version():
    actual = importlib.metadata.version("hivemind")
    if actual != HIVEMIND_VERSION:
        raise RuntimeError(f"Eclipse hooks require hivemind=={HIVEMIND_VERSION}; installed {actual}")


def peer_text(raw):
    return str(PeerID(raw)) if isinstance(raw, bytes) else str(raw)


def records(protocol, item, *, decoded=False):
    if item is None or not hasattr(item.value, "items"):
        return []
    result = []
    # TimedStorage.items() evicts expired entries. Inspect its underlying mapping
    # instead, so observation never changes local replica/cache dictionaries.
    entries = item.value.data.items() if isinstance(item.value, DictionaryDHTValue) else item.value.items()
    for subkey, entry in entries:
        try:
            value = entry.value if decoded else protocol.serializer.loads(entry.value)
            result.append({"peer_id": peer_text(subkey), "active": value,
                           "expiration_time": entry.expiration_time})
        except Exception as error:
            result.append({"subkey_hex": subkey.hex() if isinstance(subkey, bytes) else str(subkey),
                           "decode_error": type(error).__name__, "expiration_time": entry.expiration_time})
    return result


def storage_records(protocol, storage, target, now):
    """Read a live local record without triggering TimedStorage eviction."""
    item = storage.data.get(target)
    if item is None or item.expiration_time <= now:
        return []
    return [record for record in records(protocol, item) if record["expiration_time"] > now]


def filter_result(protocol, result, allowed):
    """Copy dictionary entries verbatim into a new dictionary; do not mutate storage."""
    if result.type != dht_pb2.FOUND_DICTIONARY:
        return [], []
    original = protocol.serializer.loads(result.value)
    retained, removed = [], []
    filtered = DictionaryDHTValue()
    with filtered.freeze():
        for subkey, entry in original.items():
            candidate = peer_text(subkey)
            if candidate in allowed:
                filtered.store(subkey, entry.value, entry.expiration_time)
                retained.append(candidate)
            else:
                removed.append(candidate)
        # Preserve the container expiration as well as each announcement expiration.
        filtered.latest_expiration_time = original.latest_expiration_time
    result.value = protocol.serializer.dumps(filtered)
    return retained, removed


class ObservedDHT(hivemind.DHT):
    def __init__(self, *args, eclipse_settings, eclipse_trace, malicious_averager=False, **kwargs):
        self.eclipse_settings, self.eclipse_trace = eclipse_settings, eclipse_trace
        self.malicious_averager = malicious_averager
        self.eclipse_roles = {}
        self.target_id = DHTID.generate(source=eclipse_settings.group_key)
        super().__init__(*args, **kwargs)

    def get(self, key, latest=False, return_future=False, **kwargs):
        context = averager_query.get()
        if context is not None and key == self.eclipse_settings.group_key:
            kwargs["trace_query_id"] = context["query_id"]
            kwargs["trace_search_id"] = context["search_id"]
        return super().get(key, latest=latest, return_future=return_future, **kwargs)

    async def _get(self, key, latest, future, trace_query_id=None, trace_search_id=None, **kwargs):
        if key != self.eclipse_settings.group_key:
            return await super()._get(key, latest, future, **kwargs)
        context = {"query_id": trace_query_id or str(uuid.uuid4()), "search_id": trace_search_id,
                   "responses": []}
        token = query_context.set(context)
        protocol = self._node.protocol
        now = get_dht_time()
        local = {name: storage_records(protocol, storage, self.target_id, now)
                 for name, storage in (("replica", protocol.storage), ("cache", protocol.cache))}
        self.eclipse_trace.emit("lookup_started", group_key=key, **context,
                                requester_peer_id=str(self._node.peer_id), local_records=local)
        try:
            await super()._get(key, latest, future, **kwargs)
            merged = records(protocol, future.result(), decoded=True)
            # Match sources to announcement identity; keep all raw versions in dht_response events.
            for record in merged:
                sources = [response["source"] for response in context["responses"]
                           if any(item.get("peer_id") == record.get("peer_id") for item in response["records"])]
                local_seen = any(any(item.get("peer_id") == record.get("peer_id") for item in values)
                                 for values in local.values())
                record["sources"] = sources
                record["local_copy_present"] = local_seen
                labels = {source["storage_role"] for source in sources}
                record["provenance"] = ("both" if {"honest", "malicious"} <= labels else
                                        "malicious-only" if labels == {"malicious"} and not local_seen else
                                        "honest-only" if labels == {"honest"} and not local_seen else
                                        "local/cache/unknown")
            self.eclipse_trace.emit("lookup_completed", group_key=key, query_id=context["query_id"],
                                    search_id=trace_search_id, records=merged)
        except BaseException as error:
            self.eclipse_trace.emit("lookup_failed", query_id=context["query_id"], error=type(error).__name__)
            raise
        finally:
            query_context.reset(token)

    async def _store(self, key, value, expiration_time, subkey, future, **kwargs):
        if key != self.eclipse_settings.group_key:
            return await super()._store(key, value, expiration_time, subkey, future, **kwargs)
        context = {"store_id": str(uuid.uuid4()), "group_key": key,
                   "peer_id": peer_text(subkey), "active": value, "expiration_time": expiration_time}
        token = store_context.set(context)
        self.eclipse_trace.emit("store_started", **context)
        try:
            await super()._store(key, value, expiration_time, subkey, future, **kwargs)
            self.eclipse_trace.emit("store_completed", **context, public_store_result=future.result())
            await snapshot_storage(self, self._node)
        finally:
            store_context.reset(token)

    def run(self):
        trace, target = self.eclipse_trace, self.target_id
        trace.producer_started()
        original_create = DHTNode.create.__func__

        @functools.wraps(original_create)
        async def create(cls, *args, **kwargs):
            node = await original_create(cls, *args, **kwargs)
            self.roles_ready = asyncio.Event()

            async def observe_storage():
                while node.is_alive:
                    await snapshot_storage(self, node)
                    await asyncio.sleep(self.eclipse_settings.storage_poll_seconds)

            self.storage_task = asyncio.create_task(observe_storage())
            return node

        DHTNode.create = classmethod(create)
        original_find = DHTProtocol.rpc_find

        @functools.wraps(original_find)
        async def rpc_find(protocol, request, context):
            response = await original_find(protocol, request, context)
            if self.eclipse_settings.storage == "filter" and target.to_bytes() in request.keys:
                await self.roles_ready.wait()
                allowed = {peer for peer, role in self.eclipse_roles.items()
                           if role["averager_role"] in CONTROLLED_AVERAGER_ROLES}
                for key, result in zip(request.keys, response.results):
                    if key == target.to_bytes():
                        kept, removed = filter_result(protocol, result, allowed)
                        trace.emit("response_filtered", group_key=self.eclipse_settings.group_key,
                                   requester_peer_id=str(context.remote_id), retained=kept, removed=removed)
            return response

        DHTProtocol.rpc_find = rpc_find
        original_call = DHTProtocol.call_find

        @functools.wraps(original_call)
        async def call_find(protocol, peer, keys):
            keys = list(keys)
            output = await original_call(protocol, peer, keys)
            context = query_context.get()
            if target in keys and context is not None:
                source = {"peer_id": str(peer), **self.eclipse_roles.get(str(peer), {})}
                source.setdefault("storage_role", "unknown")
                node_id = protocol.routing_table.get(peer_id=peer)
                if source.get("dht_id") is None:
                    source["dht_id"] = f"{int(node_id):040x}" if node_id is not None else None
                raw = records(protocol, output[target][0]) if output is not None else []
                context["responses"].append({"source": source, "records": raw})
                trace.emit("dht_response", query_id=context["query_id"], search_id=context["search_id"],
                           source=source, records=raw, status="responded" if output is not None else "no_response")
            return output

        DHTProtocol.call_find = call_find
        original_store = DHTProtocol.call_store

        @functools.wraps(original_store)
        async def call_store(protocol, peer, keys, values, expiration_time, subkeys=None, in_cache=None):
            # Preserve Hivemind's scalar/sequence normalization and its return value.
            keys, values = list(keys), list(values)
            output = await original_store(protocol, peer, keys, values, expiration_time, subkeys, in_cache)
            expirations = [expiration_time] * len(keys) if isinstance(expiration_time, (float, int)) else expiration_time
            caches = [in_cache or False] * len(keys) if in_cache is None or isinstance(in_cache, bool) else in_cache
            subkey_list = [subkeys] * len(keys) if subkeys is None or isinstance(subkeys, bytes) else subkeys
            for index, key in enumerate(keys):
                if key != target:
                    continue
                trace.emit("store_acknowledgement", store_id=(store_context.get() or {}).get("store_id"),
                           group_key=self.eclipse_settings.group_key, recipient_peer_id=str(peer),
                           recipient_storage_role=self.eclipse_roles.get(str(peer), {}).get("storage_role", "unknown"),
                           peer_id=peer_text(subkey_list[index]) if subkey_list[index] is not None else None,
                           expiration_time=expirations[index], in_cache=caches[index],
                           accepted=output[index] if output is not None else None,
                           evidence="remote_store_acknowledgement_not_current_holder_census")
            return output

        DHTProtocol.call_store = call_store
        super().run()
        trace.producer_finished()

    async def _shutdown(self):
        task = getattr(self, "storage_task", None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        return await super()._shutdown()


async def metadata(dht, node):
    settings = dht.eclipse_settings
    return {"peer_id": str(node.peer_id), "dht_id": f"{int(node.node_id):040x}",
            "storage_role": "malicious" if settings.storage == "filter" else "honest",
            "averager_role": averager_role(settings.averager, controlled=not settings.focal and
                                          getattr(dht, "malicious_averager", False),
                                          override=settings.averager_role_override),
            "averager_behavior": settings.averager,
            "averager_role_schema_version": 2,
            "focal_victim": settings.focal, "group_key": settings.group_key,
            "storage_observed": True,
            "matchmaking_observed": settings.focal or getattr(dht, "malicious_averager", False),
            "target_dht_id": f"{int(dht.target_id):040x}", "xor_distance": int(node.node_id) ^ int(dht.target_id),
            "placement": settings.placement, "num_replicas": node.num_replicas,
            "cache_locally": node.cache_locally, "cache_nearest": node.cache_nearest,
            "cache_on_store": node.cache_on_store, "reuse_get_requests": node.reuse_get_requests,
            "bucket_size": node.protocol.bucket_size, "num_workers": node.num_workers,
            "hivemind_version": HIVEMIND_VERSION,
            "observation_transport": getattr(dht.eclipse_trace, "TRANSPORT_VERSION", "unknown")}


async def configure_roles(dht, node, roles):
    dht.eclipse_roles = roles
    dht.roles_ready.set()


async def snapshot_storage(dht, node):
    protocol = node.protocol
    now = get_dht_time()
    dht.eclipse_trace.emit("storage_snapshot", group_key=dht.eclipse_settings.group_key,
                           holder_peer_id=str(node.peer_id), holder_dht_id=f"{int(node.node_id):040x}",
                           storage_role="malicious" if dht.eclipse_settings.storage == "filter" else "honest",
                           replica_records=storage_records(protocol, protocol.storage, dht.target_id, now),
                           cache_records=storage_records(protocol, protocol.cache, dht.target_id, now))
