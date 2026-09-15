# Run all node types from one image, selecting behaviour via parameters

## The Problem

Normal and adversarial nodes need different behaviour but share the same base
protocol implementation.

## Options Considered

- **Use one image for all node types and select the behaviour through runtime parameters**.
- Build a separate image for each node type.
- Use a generic runner image and mount the node script into the container.
  
## Rationale

The difference between an honest node and an adversarial node is something we want to change between experiments. It is therefore better to make it a runtime setting rather than something that requires building a different image.

Using one image also means we build the software once and then choose the node behaviour when the experiment starts.

We decided not to mount the node script from the host. This would make the experiment less reproducible because the code running inside the container would depend on a file on the host machine. It is useful for local development and debugging, but it should not be how the experiment is normally run.

Separate images are still supported in the configuration. This is useful when we actually need to run different versions of the software, for example testing version 0.3.0 against version 0.4.0. That is a different type of experiment from changing a node between honest and adversarial behaviour.

## Notes

- Each setting under parameters is automatically passed to the node as a PARAM_<KEY> environment variable. This means we can add new behaviour settings without changing the orchestrator.
- Image versions should be fixed to a specific tag or digest before recording results. Using latest could cause different runs to use different versions of the software, making the results difficult to compare.
- Status: Accepted.
