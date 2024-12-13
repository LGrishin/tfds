from raft import Node
from client import Client
import time



def test_4():
    nodes = []
    num_nodes = 5  # Количество узлов
    for i in range(num_nodes):
        node = Node(node_id=i, port=5555 + i, nodes_count=num_nodes)
        nodes.append(node)
        node.start()

    client = Client(nodes)
    time.sleep(5)
    nodes[0].stop()
    time.sleep(6)
    client.add_value('aaa', "AAAAA")
    client.add_value('c', "CCC")
    client.add_value('d', "DDD")
    client.add_value('e', "EEE")
    client.delete_value('c')
    client.delete_value('aaa')
    nodes[0].start()
    time.sleep(5)
    print()
    print(client.get_value('d'))
    print(client.get_value('aaa'))
    print()
    for node in nodes:
        node.stop()


test_4()