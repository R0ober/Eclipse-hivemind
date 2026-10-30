"""Aggregate declared RPC diagnostics; keep every primary measurement and error."""
from collections import Counter

SCHEMA = "rpc-counters-v1"
INTERVAL_SECONDS = 1.0
RPC_KINDS = frozenset({"lookup_started", "lookup_completed", "dht_response", "response_filtered",
                       "store_started", "store_completed", "store_acknowledgement"})


def aggregate_kind(node_id, kind):
    return kind in RPC_KINDS or (kind == "candidate_snapshot" and node_id != "victim-0")


class CounterWindow:
    """Memory depends on the declared diagnostic kinds, never the event volume."""
    def __init__(self, node_id):
        self.node_id = node_id
        self.index = 0
        self.clear()

    def clear(self):
        self.counts = Counter()
        self.first = self.last = None
        self.details = Counter()

    def add(self, kind, data, now):
        if not aggregate_kind(self.node_id, kind):
            return False
        self.counts[kind] += 1
        self.first = now if self.first is None else self.first
        self.last = now
        if kind == "response_filtered":
            self.details["filter_retained_announcements"] += len(data.get("retained", ()))
            self.details["filter_removed_announcements"] += len(data.get("removed", ()))
            self.details["filter_empty_responses"] += not data.get("retained")
        elif kind == "dht_response":
            self.details["dht_returned_records"] += len(data.get("records", ()))
            self.details["dht_empty_responses"] += not data.get("records")
            self.details["dht_no_response"] += data.get("status") == "no_response"
        return True

    def pop(self):
        if not self.counts:
            return None
        result = dict(compact_schema=SCHEMA, summary_index=self.index,
                      window_first_time=self.first, window_last_time=self.last,
                      event_counts=dict(self.counts), compacted_events=sum(self.counts.values()),
                      diagnostic_totals=dict(self.details))
        self.index += 1
        self.clear()
        return result


def valid_summary(node_id, data):
    counts = data.get("event_counts")
    return (data.get("compact_schema") == SCHEMA
            and type(data.get("summary_index")) is int and data["summary_index"] >= 0
            and isinstance(counts, dict) and bool(counts)
            and all(aggregate_kind(node_id, kind) and type(count) is int and count > 0
                    for kind, count in counts.items())
            and type(data.get("compacted_events")) is int
            and data["compacted_events"] == sum(counts.values()))
