import sys
import hivemind 
from hivemind.utils import get_dht_time


peer_addr = sys.argv[1]
print(f"--------peer adress inputed --------- \n{5*" "}addr: {peer_addr} ")
dht = hivemind.DHT(
    start=True,
    initial_peers= [peer_addr]  # [<Multiaddr /ip4/127.0.0.1/tcp/37509/p2p/12D3KooWH1wgbNmEKsu5A8Nz3j1oqUozBJqQKcqqXYRzbw8aKykb>]
    )

peer1 = dht.get("hello/from_peer1")
print(f"{5*" "}peer1 addr: {peer1}")

dht.store(
    key="hello/from_peer2",
    value="yeeting",
    expiration_time=get_dht_time() + 300

)
