# t1_single.py
import time
import hivemind
from hivemind.utils import get_dht_time
import time 


dht = hivemind.DHT(start=True)

print("my addresses:")
for addr in dht.get_visible_maddrs():
    print("   ", addr)

current_time_stamp = get_dht_time()
print(current_time_stamp)
dht.store(
    key="hello",
    value="42",
    expiration_time= current_time_stamp + 5
)

value = dht.get("hello")
print(value)

time.sleep(6)
value = dht.get("hello")
print(value)

dht.shutdown()

# in the dht store we only store a key for the specifed exp time. used so we dont need some kind of message that is 
# echoed through the whole network, or leader election (lets not talk about how to handle offline nodes). so 
# the expr time basically acts like a hearbeat 
