from node import Node
from client import Client
import time

def test_1():
    nodes_count = 3
    nodes = []
    for node_id in range(nodes_count):
        node = Node(node_id=node_id, nodes_count=nodes_count)
        nodes.append(node)
    
    time.sleep(1)
    nodes[0].isolate(1)
    # nodes[1].isolate(3)
    # nodes[2].isolate(3)
    client = Client()
    operations = [
        {'operation': 'add', 'key': 'bbbb', 'value': 'BBBB'},
        {'operation': 'add', 'key': 'aaaa', 'value': 'AAAAA'},
        {'operation': 'add', 'key': 'cccc', 'value': 'CCCC'},
        {'operation': 'delete', 'key': 'aaaa'},
        {'operation': 'add', 'key': 'aaaa', 'value': 'AAAAA'},
        {'operation': 'update', 'key': 'aaaa', 'value': 'aAaA'},
    ]
    client.patch(operations, 8080)
    time.sleep(2)
    d1 = nodes[0].get_data_storage()
    d2 = nodes[1].get_data_storage()
    d3 = nodes[2].get_data_storage()
    time.sleep(0.5)
    assert d1 == d2 == d3
    assert d1 == {'bbbb': 'BBBB', 'aaaa': 'AAAAA', 'cccc': 'CCCC'}
    # print(d1)
    print('Test 1 passed.')

test_1()