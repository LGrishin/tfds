from node import Node
from client import Client
import time
def test_2():
    nodes_count = 3
    nodes = []
    for node_id in range(nodes_count):
        node = Node(node_id=node_id, nodes_count=nodes_count)
        nodes.append(node)
    
    time.sleep(1)
    nodes[0].isolate(2)
    nodes[1].isolate(2)
    nodes[2].isolate(2)
    client = Client()

    operations = [
        {'operation': 'add', 'key': 'bbbb', 'value': 'BBBB'},
        {'operation': 'add', 'key': 'zzzz', 'value': 'ZZZZ'},
    ]
    client.patch(operations, 8080)

    operations = [
        {'operation': 'add', 'key': 'aaaa', 'value': 'AAAA'},
    ]
    client.patch(operations, 8081)

    operations = [
        {'operation': 'add', 'key': 'cccc', 'value': 'CCCC'},
    ]
    client.patch(operations, 8082)
    time.sleep(3)
    d1 = nodes[0].get_data_storage()
    d2 = nodes[1].get_data_storage()
    d3 = nodes[2].get_data_storage()
    time.sleep(0.5)
    assert d1 == d2 == d3
    # print(d1)
    assert d1 == {'bbbb': 'BBBB', 'zzzz': 'ZZZZ', 'aaaa': 'AAAA', 'cccc': 'CCCC'}
    print('Test 2 passed.')
    # print(d1)

test_2()