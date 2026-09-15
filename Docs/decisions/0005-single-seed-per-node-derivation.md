# Use a single experiment seed with per-node derivation
 
## The Problem
 
Some of the adversarial behaviour is random, so we need a way to make an experiment repeatable. At the same time, each node should have its own random behaviour rather than all nodes behaving in exactly the same way.
 
## Options Considered
 
- Do not use a seed.
- Give every node the same seed.
- **Use one seed for the whole experiment and create a separate seed for each node using hash(seed, node_id)**.

## Rationale
 
The experiment only needs one seed in the configuration. Using the same seed again allows us to reproduce the experiment later.

However, we do not want every node to use the exact same random sequence. Each node therefore creates its own seed from the experiment seed and its node_id. This keeps the nodes independent while still allowing the whole experiment to be reproduced.

It is better to include this from the beginning rather than add it later. If we run experiments without a seed and later discover that the results cannot be reproduced, it may not be possible to recreate the same behaviour.
## Notes
 
- The nodes random behaviour and its DHTID are both based on the nodes derived seed.
- The DHTID is the identity used by Kademlia when routing between nodes. It is different from the libp2p PeerID.
- The PeerID is managed separately through identity_path, which gives each node a predictable network identity.
- Status: Accepted.
