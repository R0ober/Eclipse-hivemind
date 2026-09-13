# t6_train.py
import argparse
import torch
import hivemind
from tqdm.auto import tqdm
import time 

p = argparse.ArgumentParser()
p.add_argument("--peers", nargs="*", default=None)
p.add_argument("--steps", type=int, default=500)
p.add_argument("--sleep", type=float, default=0.5,
               help="seconds to sleep per step, so you have time to launch more then one peer")

args = p.parse_args()

dht = hivemind.DHT(initial_peers=args.peers, start=True)
if not args.peers:
    print("ADDR:", dht.get_visible_maddrs()[0])

model = torch.nn.Linear(1, 1)
BATCH = 32

opt = torch.optim.SGD(model.parameters(), lr=0.001, momentum=0.9)

opt = hivemind.Optimizer(
    dht=dht,
    run_id="yeet",
    optimizer=opt,
    target_batch_size=256,
    batch_size_per_step=BATCH,
    matchmaking_time=3.0,
    averaging_timeout=10.0,
    use_local_updates=True,
    verbose=True
)


with tqdm(range(args.steps)) as progressbar:
    for step in progressbar:
        opt.zero_grad()
 
        # forward pass: learn y = 3x + 2
        x = torch.randn(BATCH, 1)
        y = 3 * x + 2 + 0.1 * torch.randn(BATCH, 1)
        loss = ((model(x) - y) ** 2).mean()
 
        # backward pass
        loss.backward()
        opt.step()


        if (step + 1) % 50 == 0:
            epoch_info = getattr(opt, 'local_epoch', step + 1)
            weight = model.weight.item()
            bias = model.bias.item()
            print(f"\nStep {step + 1} | Epoch: {epoch_info} | Loss: {loss.item():.4f} | Weight: {weight:.3f}, Bias: {bias:.3f}")

        if args.sleep:
            time.sleep(args.sleep)  ## trains to fast add sleep so other clients can join 

opt.shutdown()
dht.shutdown()
