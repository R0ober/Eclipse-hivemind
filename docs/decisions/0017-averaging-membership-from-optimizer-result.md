# Capture averaging membership from the optimizer result

## The Problem

ADR 0013 records averaging success and group size from hivemind logs. Equal group sizes hide different compositions. An honest peer with four honest members and an honest peer with four adversaries both report `group_size: 5`.

The optimizer log reports the group length without peer identities. Membership logging needs another source.

## Options Considered

- Parse member identities from hivemind status logs. The messages contain group counts, without member identities.
- Override `GradientAverager._aggregate_with_group` and read `group_info.peer_ids`. This method runs in the averager subprocess. Saving an instance attribute there does not expose the value to the runner. A return channel or subprocess log parser adds work.
- **Override `hivemind.Optimizer._average_gradients_and_load_into_optimizer` and capture the result's PeerIDs before the base method discards the members.**

## Rationale

The optimizer override reads the completed averaging result in the runner's process. `MemberCapturingOptimizer` reads `maybe_step_control.result(timeout)`, stores sorted PeerID strings and calls the base method. The completed result is a cached dictionary, so the base method reads the same value without repeating averaging.

The runner uses `pop_group_members()` to retrieve and clear the saved list. Missing, failed or empty operations produce no `group_members` field.

Analysis resolves PeerIDs through `node_started.hivemind_address`. The multiaddress includes the network PeerID. This mapping uses existing events and requires no peer-directory endpoint.

A live two-averager check confirmed a result dictionary keyed by both peers' identities. A three-node integration check captured membership in 30 of 30 rounds. Each `len(group_members)` matched `group_size`, and every PeerID resolved to a node ID.

## Notes

- The override depends on a private method verified against hivemind 1.1.12. Recheck the method and result shape after an upgrade.
- Training images currently accept `hivemind>=1.1.0`. This requirement does not enforce the verified version.
- The log parser and member capture use separate measurement paths. Compare `group_size` with `len(group_members)` to identify disagreement.
- Leader identity and the current group key need separate capture work. Neither field belongs to this implementation.
- Status: Accepted.
