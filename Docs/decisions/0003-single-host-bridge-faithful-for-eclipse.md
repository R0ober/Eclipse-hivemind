# Treat the single-host Docker bridge as a faithful environment for Kademlia eclipse experiments

## The Problem

All containers on a single Docker bridge use the same subnet (172.x.0.0/16). We need to understand whether having all nodes on the same subnet could affect the results of our eclipse attack experiments before choosing this setup.

## Options Considered

- **Use one Docker bridge network, with one container for each node**.
- Use several Docker networks with different subnets and spread the different node types across them.
- Use a multi host setup, such as Kubernetes or multiple virtual machines.
  
## Rationale

Hivemind's DHT is based on Kademlia. In Kademlia, nodes are organized based on their DHT IDs. These IDs are independent of the nodes' IP addresses, so the fact that all containers are on the same subnet does not affect how peers are selected or how the routing table works.

The eclipse attack also works through the DHT ID space rather than through IP addresses. This means that putting all nodes on the same /16 subnet does not change the way the attack works.

The original concern about using the same subnet comes from systems such as Bitcoin, where IP diversity can be used as part of the peer-selection and protection mechanisms. This is different from Kademlia, where routing is based on DHT IDs. Kademlia based defences such as S/Kademlia also work in the ID space rather than using different IP ranges.

Therefore, using multiple Docker subnets would not provide any benefit for the standard Hivemind DHT. Different subnets would only become important if we added a defence that specifically used IP address diversity, which vanilla Hivemind does not.

## Notes

- There is one limitation that is separate from the subnet question: because all containers run on the same host, the experiment does not provide realistic network latency, packet loss, or network partitions. Therefore, measurements that depend on these conditions may be affected by the test environment. If we need to study these effects later, we can add network simulation using tools such as tc/netem or Pumba.
- The Docker setup used for one bridge network can later be adapted to a larger cluster, with one node per pod or container, if more scale is needed.
- This decision should also be kept in mind when presenting the experiment results, since a natural question will be why all nodes are running on the same subnet.
- Status: Accepted.
