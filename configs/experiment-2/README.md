# Experiment 2 — averaging group size

Same reversal attack as experiment 1, but groups of 5 instead of the whole swarm.
Cells: 0, 2, 4, 6 malicious out of 10.

`target_group_size` and `min_group_size` are both 5, so a round either averages
exactly five peers or falls back to local gradients (ADR 0016). Hivemind's
default would mix group sizes by timing, which would muddy the one thing this
experiment is varying. Fallbacks show up as `fallback: true` i.e check averaging
success before comparing losses.


Group *composition* still varies. 4 malicious out of 10 can land 0–4 of them in a
group of 5. That's the first eclipse-ish thing here: who you average with, not
the global count. One run per cell, so a small gap between neighbours could just
be who met whom.

The logs record group size, not membership. An honest peer averaging with four
honest peers vs four adversaries looks identical in the data. Peer IDs on
`averaging_completed` would be needed for a real eclipse result.
