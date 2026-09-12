import argparse
import torch
import hivemind
from hivemind import DecentralizedAverager

p = argparse.ArgumentParser()
p.add_argument("--value", type=float, required=True)
p.add_argument("--peers", nargs="*", default=None)
args = p.parse_args()

dht = hivemind.DHT(initial_peers=args.peers, start=True)
if not args.peers:
    print("ADDR:", dht.get_visible_maddrs()[0])

tensors = [torch.ones(3) * args.value]


averager = DecentralizedAverager(
    averaged_tensors =  tensors,
    dht= dht,
    prefix="something?",
    target_group_size=2,
    start=True
)
print(tensors)
avg_bool = averager.step(wait=True)
print(avg_bool)

with averager.get_tensors() as t:
    print(t[0])


# run twice with different --value

## {<libp2p.peer.id.ID (12D3KooWSzcuEqtgFA34YbLrbVHEMzdjfHktQt6bCjqXU4tPG1cg)>: None, <libp2p.peer.id.ID (12D3KooWPf6iKZgo9BfrkDbo5Hi54iGPbtT 
#where its none now hivemind will use that for things like peers sample count 

## if we just run one python3 toy_hivemind/t4.avg.py --value 1 it sits forever waitng for target group size given that wait=True
