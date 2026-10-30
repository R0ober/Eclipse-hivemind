# Eclipse leader attack

## Goal

Make an adversarial leader admit one victim and its allies, then measure isolation across rounds as the averaging key changes.

## Status

This document is a proposal for a malicious leadership/admission mechanism in this repository snapshot, not a record of demonstrated attack success. Membership logging is available. The separate controlled-composition experiment uses gradient namespaces and does not implement these admission changes. Chosen DHTIDs apply to the record-poisoning variant rather than the first leader-only experiment.

## Source findings from hivemind 1.1.12

matchmaking.py line 478 orders leadership candidates by declared expiration, followed by PeerID bytes. Test an earlier declared expiration to select an adversarial leader.

Matchmaking._check_reasons_to_reject at line 334 receives the joining PeerID through context.remote_id. Override admission in the adversary runner to restrict membership to the victim and allies.

The default initial_group_bits value is empty. The plan proposes non-empty bits to exercise group-key changes. Verify how members calculate their next key before using this setting in durability measurements.

The averager subprocess constructs Matchmaking internally. Apply the admission patch at module load in the adversary runner and verify the child process uses the patch.

## Implementation tasks

Add peer-directory access for runtime node IDs and PeerIDs. Prefer startup-event data rather than another identity source. The directory belongs to coordination and should expose no attack policy.

Create an adversary-eclipse-leader runner and image. The runner obtains victim and ally identities, changes admission and declares the intended expiration.

Expose initial_group_bits as an experiment parameter. Keep attack timing and key movement inside the node.

Measure the number of rounds before the victim averages with another honest peer. Use `group_members` to calculate this value.

## Verification order

Start with one round. Verify the victim's group contains the victim and adversaries, while other honest peers form a separate group. Then add key movement and measure isolation duration across rounds.

## Resolve these questions before claiming targeted isolation

Trace leader_assemble_group and rpc_join_group to verify admission excludes the selected PeerIDs.

Verify the subprocess uses the runner's admission patch.

Check follower expiration against BAD_EXPIRATION_TIME at line 358. Followers need an expiration later than the leader's declared expiration.

## Acceptance

The first stage records a victim group containing only the victim and adversaries. Other honest peers form their own group. The second stage plots isolation duration against group-key movement or churn, with repeated runs.

## Research scope

Run the experiment against your own Docker swarm. Use the results to measure the weakness and evaluate defences.
