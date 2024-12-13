from raft import Node
from client import Client
import time


def test_1():
    nodes = []
    num_nodes = 5  # Количество узлов
    for i in range(num_nodes):
        node = Node(node_id=i, port=5555 + i, nodes_count=num_nodes)
        nodes.append(node)
        node.start()

    client = Client(nodes)
    time.sleep(5)
    client.add_value('aaa', 'AAAA')
    client.add_value('bbb', 'BBBB')
    client.add_value('ccc', 'CCCC')
    time.sleep(2)
    client.put('aaa', 'TTTT')
    client.put('ddd', 'TTTT')
    client.put('bbb', 'OOO')
    time.sleep(2)
    print(client.get_value('aaa'))
    print(client.get_value('bbb'))
    print(client.get_value('ccc'))
    time.sleep(2)
    
    for node in nodes:
        node.stop()


    
test_1()
