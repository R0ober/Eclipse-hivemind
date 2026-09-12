import argparse
import torch
import hivemind
from hivemind import DecentralizedAverager

p = argparse.ArgumentParser()
p.add_argument("--value", type=float, required=True)
p.add_argument("--peers", nargs="*", default=None)
p.add_argument("--weight", type=float)
args = p.parse_args()

dht = hivemind.DHT(initial_peers=args.peers, start=True)
if not args.peers:
    print("ADDR:", dht.get_visible_maddrs()[0])

tensors = [torch.ones(3) * args.value]


averager = DecentralizedAverager(
    averaged_tensors =  tensors,
    dht= dht,
    prefix="something?",
    target_group_size=3,
    averaging_alpha=0.5,
    start=True
)
print(tensors)
avg_bool = averager.step(weight=args.weight)
print(avg_bool)

with averager.get_tensors() as t:
    print(t[0])


# run twice with different --value and diffrent --weight tried: value 1 weight 3.0 and value 3 and weight 3 i.e mirrored
# with   target_group_size=3, doesnt wait instantly averages
# with    min_group_size=3, we get that step waits 
# alpha     works as expected scale how much you move towards the average i.e value 3 peer: 3+0.5(1.5-3)=2.25
#                                                                             value 1 peer: 1+0.5(1.5-1)=1.25
# with alpha = 1.0                                                            value 3 peer: 3+(1.5-3)=1.5
#                                                                             value 1 peer: 1+(1.5-1)=1.5                                            
