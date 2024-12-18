from node import Node
from client import Client
import time

def test_6():
    nodes_count = 3
    nodes = []
    for node_id in range(nodes_count):
        node = Node(node_id=node_id, nodes_count=nodes_count, random_crash=True)
        nodes.append(node)

    
    time.sleep(1)
    
    client = Client()
    operations = [
        {'operation': 'add', 'key': 'aaaa', 'value': 'AAAA'},
    ]
    client.patch(operations, 8080)

    operations = [
        {'operation': 'add', 'key': 'aaaa', 'value': 'BBBB'},
    ]
    client.patch(operations, 8081)

    operations = [
        {'operation': 'add', 'key': 'aaaa', 'value': 'CCCC'},
    ]
    client.patch(operations, 8082)
    
    operations = [
        {'operation': 'update', 'key': 'aaaa', 'value': 'VVV'},
    ]
    client.patch(operations, 8081)
    
    operations = [
        {'operation': 'update', 'key': 'aaaa', 'value': 'PPPP'},
    ]
    client.patch(operations, 8080)

    for node_id in range(len(nodes)):
        print(f'Node {node_id} data store is {nodes[node_id].data_store}')
    time.sleep(3)
    # print('--------')
    # for node_id in range(len(nodes)):
    #     print(f'Node {node_id} data store is {nodes[node_id].data_store}')

    d1 = nodes[0].get_data_storage()
    d2 = nodes[1].get_data_storage()
    d3 = nodes[2].get_data_storage()
    # assert d1 == d2 == d3
    
    # assert d1 == {'aaaa': 'PPPP'}
    # time.sleep(3)
    print('-------------')
    operations = [
        {'operation': 'delete', 'key': 'aaaa'},
    ]
    client.patch(operations, 8082)
    time.sleep(15)

    d1 = nodes[0].get_data_storage()
    d2 = nodes[1].get_data_storage()
    d3 = nodes[2].get_data_storage()

    assert d1 == d2 == d3
    # print(d1)
    assert d1 == {}
    time.sleep(0.5)
    print('Test 6 passed')

test_6()