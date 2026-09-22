#!/usr/bin/env bash

set -u

run_id=""

docker events \
  --filter type=container \
  --filter event=create \
  --filter event=start \
  --filter event=destroy \
  --format '{{.Time}} {{.Action}} {{.Actor.Attributes.name}}' |
while read -r timestamp action container; do

    # Extract run-20260918-224008-031190
    current_run_id=$(echo "$container" | sed -E 's/^(run-[^-]+-[^-]+-[^-]+)-.*/\1/')

    # First container tells us which run we're watching
    if [[ -z "$run_id" ]]; then
        run_id="$current_run_id"
        echo "Following run: $run_id"
    fi

    # Ignore containers from other runs
    [[ "$current_run_id" != "$run_id" ]] && continue

    case "$action" in
        create|start)
            echo ">>> $action $container"

            docker logs -f "$container" 2>&1 |
                sed "s/^/[$container] /" &
            ;;

        destroy)
            echo "<<< destroy $container"
            ;;
    esac
done

wait
