# Pin the averaging group to an exact size rather than a maximum

## The Problem

Experiment 2 varies the averaging group size while holding the malicious ratio
fixed, so group size has to be something the config sets. Hivemind's
`target_group_size` does not set it. It is documented as *"attempts to form
groups with up to this many peers"*, and `min_group_size` defaults to 2: when
`matchmaking_time` runs out, the leader averages with whoever turned up.

A run configured for groups of five therefore produces a mix of groups of five,
four, three and two, decided by matchmaking timing. Group size stops being an
independent variable and becomes a confound, and a result comparing group sizes
cannot be argued from.

## Options Considered

- Leave `min_group_size` at 2 and report the realised size as a measured value.
- **Expose both `target_group_size` and `min_group_size` as node parameters, and
  set them equal in the experiments that need an exact group size.**
- Reimplement matchmaking to hold rounds open until the group is full.

## Rationale

Reporting the realised size keeps every round usable but leaves the analysis
comparing runs whose group sizes overlap. Separating "groups of five" from
"groups of three" then means splitting rounds after the fact and having fewer of
each, which is a weaker claim than running the experiment at a fixed size.

Both are plain parameters in the config rather than one derived from the other,
so a config says what it means and a future experiment can set them apart if it
wants a range instead of a size. Setting them equal makes group size exact. A round that
cannot assemble the full group disbands and falls back to local gradients, and
that is reported as a fallback with hivemind's own reason rather than silently
averaging a smaller group. The failure is visible in the events instead of
hiding inside a distribution.

The cost is rounds that do not average when matchmaking is tight, which shows up
as a lower averaging success rate rather than as a distorted group size. That is
the right trade for this experiment: a failed round is a known gap, an
unexpectedly small group is a wrong number.

Measured on six peers with `target_group_size: 3`, all 36 rounds averaged at
exactly 3 with no fallbacks.
