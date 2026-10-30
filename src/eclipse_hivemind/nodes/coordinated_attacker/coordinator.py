"""Node-side client for the private coordination service."""

from __future__ import annotations

import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class CoordinatorClient:
    def __init__(self, endpoint: str, experiment_id: str, timeout: float = 3.0) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.experiment_id = experiment_id
        self.timeout = timeout

    def _url(self, suffix: str) -> str:
        return f"{self.endpoint}/api/v1/experiments/{self.experiment_id}{suffix}"

    def _request(self, method: str, suffix: str, payload: dict | None = None) -> dict:
        request = Request(
            self._url(suffix),
            data=json.dumps(payload).encode("utf-8") if payload is not None else None,
            headers={"Content-Type": "application/json"},
            method=method,
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"coordinator returned HTTP {error.code}: {body}") from error
        except (URLError, TimeoutError) as error:
            raise RuntimeError(f"could not reach coordinator at {self.endpoint}: {error}") from error

    def configure_plan(self, victim_peer_id: str, recruiter_peer_ids: list[str]) -> dict:
        return self._request("PUT", "/plan", {"victim_peer_id": victim_peer_id,
                                                 "recruiter_peer_ids": recruiter_peer_ids})

    def active_claim(self) -> dict | None:
        return self._request("GET", "/claim")["claim"]

    def claim(self, leader_peer_id: str, requester_peer_id: str, expires_at: float) -> dict:
        return self._request("POST", "/claims", {"leader_peer_id": leader_peer_id,
                                                    "requester_peer_id": requester_peer_id,
                                                    "expires_at": expires_at})

    def finish(self, claim_id: str, outcome: str, *, group_id: str | None = None,
               reason: str | None = None) -> dict:
        return self._request("POST", f"/claims/{claim_id}", {"outcome": outcome,
                                                                 "group_id": group_id, "reason": reason})
