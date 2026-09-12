import sys
import time
import hivemind
from hivemind.utils import get_dht_time

name = sys.argv[1]
peers = sys.argv[2:] or None

dht = hivemind.DHT(initial_peers=peers, start=True)

if not peers:
    print("addr:", dht.get_visible_maddrs()[0])
    
print("Looping")
while True:
    dht.store(
        key="yeet/members",
        subkey=name,
        value=time.time(),
        expiration_time= get_dht_time() + 10
    )

    members = dht.get("yeet/members", latest=True)
    #print(f"{5*" "}peer1 addr: {members}")
    something = members.value
    print("Members:")
    for k , v in something.items():
        print(f"{5* " "} Member: {k}")
        print(f"{5* " "} Entry:  {v}")


    time.sleep(2)


## spawn 3, kill one, check
