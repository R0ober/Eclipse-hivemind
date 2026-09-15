# Treat bootstrap topology as a configurable experiment variable
 
## The Problem
 
How many peers a node knows when it starts can affect whether we can see an eclipse attack. Because of this, it should be something we can change in the experiment configuration instead of something fixed in the code.

## Options Considered
 
- Give every node the full list of peers.
- Give every node only one seed peer.
- **Add a bootstrap.policy setting (seed_only or full) to the config, along with the seed to use**.

## Rationale

If every node starts with the full list of peers, the honest peers are already connected to each other. This makes it difficult for an attacker to replace them with malicious peers, so an eclipse attack may not be possible.

If nodes start with only a seed peer, they have to discover the other peers normally. This is the part of the system that an eclipse attack tries to interfere with, so this setup is better for testing the attack.

For this reason, the bootstrap setup should be controlled through the config. We can then switch between the different experiment setups without changing the code.
 
## Notes
 
- The code that gets the seed address and gives it to the nodes is handled by the orchestrator. This does not need to be configurable.
- The config controls which bootstrap policy is used and which seed is selected.
- The seed can be either an honest or adversarial node, so we can test both cases later.
- Status: Accepted.
 
