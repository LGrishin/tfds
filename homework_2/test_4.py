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
    time.sleep(5)
    client.add_value('aaa', "AAAAA")
    time.sleep(5)
    nodes[0].start()
    time.sleep(5)

    for node in nodes:
        node.stop()


test_4()