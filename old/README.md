# Local Hivemind Swarm Starter

A scalable, observable Docker Compose testbed for running decentralized ML training swarms with **[Hivemind](https://github.com/learning-at-home/hivemind)**.

## Architecture

```
                    +---------------------------+
                    |  Bootstrap Node (DHT)     |
                    |   (service: bootstrap)    |
                    +-------------+-------------+
                                  |
              +-------------------+-------------------+
              |                   |                   |
    +---------v-------+ +---------v-------+ +---------v-------+
    |  Peer 1         | |  Peer 2         | |  Peer N         |
    |  (service: peer)| |  (service: peer)| |  (service: peer)|
    |  PyTorch SGD    | |  PyTorch SGD    | |  PyTorch SGD    |
    +---------+-------+ +---------+-------+ +---------+-------+
              |                   |                   |
              +<=================>+<=================>+
                    All-Reduce Averaging Swarm
```

- **Bootstrap Node (`bootstrap`):** A lightweight Kademlia DHT node that listens on port 1337 and writes its reachable multiaddress into a shared rendezvous volume.
- **Worker Peers (`peer`):** Training nodes that connect to the bootstrap address, advertise their own container IPs, train a local 2D classifier, and periodically average weights with other peers via Hivemind's All-Reduce protocol.
- **Dynamic Scaling:** You can spin up as many peers as you need using Docker Compose's `--scale peer=N` flag.

## Prerequisites

1. **Docker Desktop** installed and running on your Mac.
2. In your terminal, navigate to this directory:
   ```bash
   cd hivemind-starter
   ```

## Step-by-Step Commands & Explanation

### 1. Build the Docker Image
```bash
docker compose build
```
* **What it does:** Reads `Dockerfile` and builds a Linux container image containing Python 3.11, PyTorch (CPU), and `hivemind==1.1.12`.
* **Why:** Packages all dependencies inside an isolated Linux environment with Apple Silicon / ARM64 compatibility.

### 2. Start the Swarm with N Peers
To start the bootstrap node plus **3 worker peers**:
```bash
docker compose up --scale peer=3
```
To run with **5 peers**:
```bash
docker compose up --scale peer=5
```
* **What it does:**
  1. Starts the `bootstrap` container first.
  2. Waits until the bootstrap healthcheck passes (rendezvous address published).
  3. Launches the requested number of `peer` containers concurrently.
  4. Streams combined logs to your terminal.

### 3. Viewing & Filtering Logs
In a second terminal window:
* View only worker logs:
  ```bash
  docker compose logs -f peer
  ```
* View bootstrap logs:
  ```bash
  docker compose logs -f bootstrap
  ```

### 4. Stopping and Resetting
To stop all containers and remove the temporary rendezvous volume:
```bash
docker compose down -v
```
* **`down`**: Gracefully terminates all running peer and bootstrap containers.
* **`-v`**: Cleans up the `rendezvous` volume so subsequent runs start with fresh routing tables.

## What to Observe in the Logs

1. **Peer Discovery:**
   Peers emit JSON logs on startup containing their unique PeerID and multiaddress:
   ```json
   {"event": "peer_started", "hostname": "...", "peer_id": "Qm...", "ip": "172.x.x.x"}
   ```
2. **Hivemind Matchmaking & All-Reduce Averaging:**
   When `target_batch_size` is accumulated, Hivemind triggers matchmaking and forms an averaging group:
   `DecentralizedAverager` logs show the group leader and merging of model weights across peers.
3. **Accuracy & Loss:**
   Every 15 steps, peers report held-out accuracy and active peers discovered in the DHT registry:
   ```json
   {"event": "step_metrics", "step": 15, "loss": 0.421, "test_accuracy": 0.895, "discovered_swarm_peers": 3}
   ```
