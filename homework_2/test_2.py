from raft import Node
from client import Client
import time



def test_2():
    nodes = []
    num_nodes = 5  # Количество узлов
    for i in range(num_nodes):
        node = Node(node_id=i, port=5555 + i, nodes_count=num_nodes)
        nodes.append(node)
        node.start()


    client = Client(nodes)
    time.sleep(5)
    client.add_value('aaa', 'AAAA')
    time.sleep(2)
    print(client.get_value('aaa'))
    time.sleep(2)

    for node in nodes:
        node.stop()

test_2()