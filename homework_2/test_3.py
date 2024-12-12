from raft import Node
from client import Client
import time



def test_3():
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
    time.sleep(5)
    nodes[2].stop()
    nodes[0].stop()
    nodes[4].stop()
    time.sleep(5)
    print('--------------------------------')
    nodes[2].start()
    client.add_value('cc', 'CCC')
    time.sleep(5)
    nodes[0].start()
    nodes[4].start()
    time.sleep(5)
    print('=================================')
    print(client.get_value('cc'))
    nodes[0].stop()
    nodes[4].stop()
    time.sleep(5)
    client.delete_value('cc')
    time.sleep(5)
    nodes[0].start()
    nodes[4].start()
    time.sleep(5)
    
    for node in nodes:
        node.stop()



test_3()