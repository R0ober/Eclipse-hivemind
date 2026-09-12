import hivemind 
from hivemind.utils import get_dht_time
import time 

dht = hivemind.DHT(start=True)
print(f"Addr: {dht.get_visible_maddrs()}")

dht.store(
    key = "hello/from_peer1",
    value="yeet",
    expiration_time=get_dht_time()+ 300
    )
print(f"Looping:")
while True:
    plain  = dht.get("hello/from_peer2")
    latest = dht.get("hello/from_peer2", latest=True) # if true is on we skip local cache nad go and ask the node directly
    print(f"plain={plain}  latest={latest}")
    time.sleep(2)

dht.shutdown()



