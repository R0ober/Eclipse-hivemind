# Capture node addresses from container logs rather than a shared volume

## The Problem

The orchestrator has to tell each node how to reach its peers, but a node's
multiaddress is not known until the node starts, because Hivemind generates the
PeerID at startup. The orchestrator needs a way to read that address back out of a
running container.

## Options Considered

- Write the address to a file on a shared Docker volume and read the file from the host.
- **Print the address on stdout behind a known marker and read it from the container log**.
- Generate the identity before startup so the address is known in advance.

## Rationale

The shared volume was how the earlier prototype worked, in `old/bootstrap.py`. It
needs a volume mounted into every container, a naming scheme so we can tell which
node wrote which file, and a cleanup step so a stale file from a previous run is not
mistaken for a fresh one. That is three moving parts for one string.

The log marker needs no shared state. The node prints one line and the orchestrator
reads it through the Docker API it is already using. The same mechanism works for
the fake test nodes, so the startup path can be exercised before the real node image
exists.

Generating identities in advance is the better long-term answer, because then every
address is known before anything starts and nodes do not have to start one at a
time. We did not choose it now because it means creating libp2p keys outside
Hivemind and mounting them into the containers, which is more work than the current
experiments need.

## Notes

- The marker `HIVEMIND_MADDR=` is a contract between `runner.py` and the
  orchestrator. Changing the text breaks startup, so it should stay stable.
- Consequence: seed nodes start one at a time, because the orchestrator waits for
  each address before starting the next. ADR 0007 lists this as a possible limit at
  large node counts.
- A node that dies before printing the marker is caught by checking the container
  status while waiting, so a crash fails immediately instead of waiting out the
  readiness timeout.
- Pre-generated identities are also what `bootstrap.policy: full` needs, see ADR
  0002. If we implement `full`, this decision should be revisited at the same time.
- Status: Accepted.
