from node import Node
from client import Client
import time

def test_3():
    nodes_count = 2
    nodes = []
    for node_id in range(nodes_count):
        node = Node(node_id=node_id, nodes_count=nodes_count)
        nodes.append(node)

    
    time.sleep(1)
    
    client = Client()
    operations = [
        {'operation': 'add', 'key': 'bbbb', 'value': 'BBBB'},
    ]
    client.patch(operations, 8080)
    time.sleep(0.5)
    
    nodes[0].isolate(2)
    nodes[1].isolate(2)


    operations = [
        {'operation': 'update', 'key': 'bbbb', 'value': 'bb'},
    ]
    client.patch(operations, 8080)

    operations = [
        {'operation': 'update', 'key': 'bbbb', 'value': 'b'},
    ]
    client.patch(operations, 8080)
    
    operations = [
        {'operation': 'add', 'key': 'aaa', 'value': 'ZZ'},
    ]
    client.patch(operations, 8081)
    
    operations = [
        {'operation': 'update', 'key': 'aaa', 'value': 'VVV'},
    ]
    client.patch(operations, 8081)
    
    # for node_id in range(len(nodes)):
    #     print(f'Node {node_id} data store is {nodes[node_id].data_store}')
    time.sleep(3)
    d1 = nodes[0].get_data_storage()
    d2 = nodes[1].get_data_storage()
    assert d1 == d2
    assert d1 == {'bbbb': 'b', 'aaa': 'VVV'}
    # print(d1)
    # for node_id in range(len(nodes)):
    #     print(f'Node {node_id} data store is {nodes[node_id].data_store}')
    assert client.get('aaa', 8081) == 'VVV'
    time.sleep(0.5)
    print('Test 3 passed')

test_3()