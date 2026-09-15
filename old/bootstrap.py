"""Lightweight Hivemind DHT Bootstrap Node for local Docker swarm."""
import json
import os
import signal
import socket
import sys
import time
from pathlib import Path

import hivemind


def main():
    rendezvous_dir = Path("/rendezvous")
    rendezvous_dir.mkdir(parents=True, exist_ok=True)
    maddr_file = rendezvous_dir / "bootstrap.maddr"
    json_file = rendezvous_dir / "initial_peers.json"

    # Remove stale rendezvous files from prior runs
    maddr_file.unlink(missing_ok=True)
    json_file.unlink(missing_ok=True)

    # Get container IP in Docker bridge network
    ip = socket.gethostbyname(socket.gethostname())
    port = int(os.environ.get("BOOTSTRAP_PORT", 1337))

    print(f"[bootstrap] Starting Hivemind DHT bootstrap node on {ip}:{port}...", flush=True)

    dht = hivemind.DHT(
        start=True,
        initial_peers=[],
        host_maddrs=[f"/ip4/0.0.0.0/tcp/{port}"],
        announce_maddrs=[f"/ip4/{ip}/tcp/{port}"],
        client_mode=False,
    )

    try:
        visible_maddrs = [str(addr) for addr in dht.get_visible_maddrs()]
        print(f"[bootstrap] Bootstrap node active with visible addresses: {visible_maddrs}", flush=True)

        # Write reachable multiaddresses to shared volume atomically
        tmp_maddr = maddr_file.with_suffix(".tmp")
        tmp_maddr.write_text("\n".join(visible_maddrs))
        tmp_maddr.replace(maddr_file)

        tmp_json = json_file.with_suffix(".tmp")
        tmp_json.write_text(json.dumps(visible_maddrs))
        tmp_json.replace(json_file)

        print("[bootstrap] Wrote rendezvous addresses to /rendezvous/. Ready for peers.", flush=True)

        def sig_handler(sig, frame):
            print(f"[bootstrap] Received signal {sig}, shutting down...", flush=True)
            dht.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGINT, sig_handler)
        signal.signal(signal.SIGTERM, sig_handler)

        while True:
            time.sleep(10)
    except Exception as e:
        print(f"[bootstrap] Error: {e}", flush=True)
        dht.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    main()
