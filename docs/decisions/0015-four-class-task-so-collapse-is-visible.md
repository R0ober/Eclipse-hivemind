# Use four classes so prediction collapse is visible

## The Problem

The original task had two balanced classes. A model that collapses to one class
then scores about 50%, which is also chance accuracy. In a gradient-reversal
run, accuracy could rise even while evaluation loss rose and every prediction
collapsed onto one class.

We need an outcome that makes collapse clearly different from learning.

## Options Considered

- Keep two classes and balance the held-out set.
- Keep two classes and report accuracy relative to the collapse floor.
- **Use four classes, one per quadrant, and report the predicted-class spread.**

## Rationale

The held-out set was already balanced, so balancing it cannot separate chance
accuracy from a constant two-class predictor. Reporting accuracy relative to a
derived floor would work, but makes the result harder to read.

With four classes, a constant predictor has an accuracy floor near 25%. The
task still needs the hidden layer, while `predicted_class_fractions` shows a
collapse directly. `eval_loss` remains the primary measure when accuracy and
the prediction spread disagree.

## Decision

- Classify points into four quadrants instead of two classes.
- Report `predicted_class_fractions` with each shared evaluation result.
- Use `eval_loss` and `eval_accuracy` to compare runs; use the class fractions
  to identify a model that has collapsed to one output class.

## Notes

- Honest node types must use compatible model architecture and task definition
  for hivemind gradient averaging to be meaningful.
- The task remains in each node type's runner. An adversarial runner may
  deliberately use different data or training behaviour, so it should not be
  constrained by a shared task module.
- `model_fingerprint` in `node_started` detects peers that began from different
  weights.
- Status: Accepted.
