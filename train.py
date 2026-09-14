"""Hivemind training peer with observable DHT and averaging interactions."""
import hashlib
import json
import os
import socket
import sys
import time
from pathlib import Path

import hivemind
import torch
from torch import nn


def wait_for_bootstrap(rendezvous_dir: Path, timeout: float = 60.0) -> list[str]:
    json_file = rendezvous_dir / "initial_peers.json"
    maddr_file = rendezvous_dir / "bootstrap.maddr"
    start_time = time.monotonic()

    while time.monotonic() - start_time < timeout:
        if json_file.exists() and json_file.stat().st_size > 0:
            try:
                peers = json.loads(json_file.read_text())
                if peers:
                    return peers
            except Exception:
                pass
        elif maddr_file.exists() and maddr_file.stat().st_size > 0:
            peers = [line.strip() for line in maddr_file.read_text().splitlines() if line.strip()]
            if peers:
                return peers
        time.sleep(1)

    raise TimeoutError(f"Timed out waiting for bootstrap address in {rendezvous_dir} after {timeout}s")


def main():
    hostname = socket.gethostname()
    peer_env = os.environ.get("PEER_NUMBER")
    if peer_env:
        peer_idx = int(peer_env)
    else:
        peer_idx = int(hashlib.md5(hostname.encode()).hexdigest()[:6], 16) % 10000

    torch.set_num_threads(1)
    rendezvous_dir = Path("/rendezvous")

    print(f"[{hostname}] Waiting for bootstrap node...", flush=True)
    initial_peers = wait_for_bootstrap(rendezvous_dir)
    print(f"[{hostname}] Discovered initial peers: {initial_peers}", flush=True)

    ip = socket.gethostbyname(hostname)
    dht = hivemind.DHT(
        start=True,
        initial_peers=initial_peers,
        host_maddrs=["/ip4/0.0.0.0/tcp/1337"],
        announce_maddrs=[f"/ip4/{ip}/tcp/1337"],
        client_mode=False,
    )

    opt = None
    try:
        visible_maddrs = [str(addr) for addr in dht.get_visible_maddrs()]
        peer_id_str = str(dht.peer_id)
        print(json.dumps({
            "event": "peer_started",
            "hostname": hostname,
            "peer_idx": peer_idx,
            "peer_id": peer_id_str,
            "ip": ip,
            "visible_maddrs": visible_maddrs
        }), flush=True)

        # Base model initialization is identical across peers
        torch.manual_seed(42)
        model = nn.Sequential(nn.Linear(2, 16), nn.ReLU(), nn.Linear(16, 2))

        # Each peer gets a distinct, reproducible slice of training samples
        sample_generator = torch.Generator().manual_seed(1000 + peer_idx)
        # Shared evaluation set to measure global generalization
        eval_generator = torch.Generator().manual_seed(9999)
        test_x = torch.randn(1024, 2, generator=eval_generator)
        test_y = (test_x[:, 0] + test_x[:, 1] > 0).long()

        opt = hivemind.Optimizer(
            dht=dht,
            run_id="dit616-toy-v1",
            optimizer=torch.optim.SGD(model.parameters(), lr=0.05),
            batch_size_per_step=32,
            target_batch_size=2048,
            use_local_updates=True,
            matchmaking_time=4.0,
            averaging_timeout=25.0,
            verbose=True,
        )

        # Brief pause to let initial DHT discovery establish routing entries
        time.sleep(5)
        dht.store(
            "dit616-swarm-registry",
            subkey=peer_id_str,
            value=f"active-peer-{peer_idx}@{ip}",
            expiration_time=time.time() + 600
        )

        started = time.monotonic()
        step = 0
        while time.monotonic() - started < 300:
            x = torch.randn(32, 2, generator=sample_generator)
            y = (x[:, 0] + x[:, 1] > 0).long()

            opt.zero_grad()
            logits = model(x)
            loss = nn.functional.cross_entropy(logits, y)
            loss.backward()
            opt.step()

            if step % 15 == 0:
                with torch.no_grad():
                    acc = (model(test_x).argmax(dim=1) == test_y).float().mean().item()
                registry = dht.get("dit616-swarm-registry", latest=True)
                active_count = len(registry.value) if registry and registry.value else 1
                print(json.dumps({
                    "event": "step_metrics",
                    "hostname": hostname,
                    "peer_idx": peer_idx,
                    "peer_id": peer_id_str,
                    "step": step,
                    "epoch": opt.local_epoch,
                    "loss": round(loss.item(), 4),
                    "test_accuracy": round(acc, 4),
                    "discovered_swarm_peers": active_count
                }), flush=True)

            step += 1
            time.sleep(0.1)

        print(f"[{hostname}] Completed 5-minute training cycle.", flush=True)
    finally:
        if opt is not None:
            opt.shutdown()
        dht.shutdown()
        print(f"[{hostname}] Shutdown cleanly.", flush=True)


if __name__ == "__main__":
    main()
