from node import Node, wait_consistency
from client import Client
import time

        
def test_8():
    nodes_count = 5
    nodes = []
    for node_id in range(nodes_count):
        node = Node(node_id=node_id, nodes_count=nodes_count, random_crash=True)
        nodes.append(node)

    print('Phase 1')

    time.sleep(1)
    client = Client()

    operations = [
        {'operation': 'add', 'key': 'aaaa', 'value': 'AAAA'},
    ]
    client.patch(operations, 8080)
    operations = [
        {'operation': 'add', 'key': 'bbbb', 'value': 'BBBB'},
    ]
    client.patch(operations, 8080)

    operations = [
        {'operation': 'add', 'key': 'aaaa', 'value': 'QQQQ'},
    ]
    client.patch(operations, 8082)
    operations = [
        {'operation': 'add', 'key': 'mmmm', 'value': 'MMMM'},
    ]
    client.patch(operations, 8082)

    operations = [
        {'operation': 'add', 'key': 'tttt', 'value': 'TTTT'},
    ]
    client.patch(operations, 8082)
    time.sleep(1)
    print(f'Node 1 {nodes[0].get_data_storage()}')
    print(f'Node 2 {nodes[1].get_data_storage()}')
    print(f'Node 3 {nodes[2].get_data_storage()}')
    print(f'Node 4 {nodes[3].get_data_storage()}')
    print(f'Node 5 {nodes[4].get_data_storage()}')

    wait_consistency(nodes)
    d1 = nodes[0].get_data_storage()
    d2 = nodes[1].get_data_storage()
    d3 = nodes[2].get_data_storage()
    d4 = nodes[3].get_data_storage()
    d5 = nodes[4].get_data_storage()
    time.sleep(0.5)
    assert d1 == d2 == d3 == d4 == d5
    print(f'All consistent: common data = {d1}')
    assert d1 == {'aaaa': 'AAAA', 'bbbb': 'BBBB', 'mmmm': 'MMMM', 'tttt': 'TTTT'}
    
    print('Phase 2')
    time.sleep(1)

    operations = [
        {'operation': 'update', 'key': 'aaaa', 'value': 'aAaA'},
    ]
    client.patch(operations, 8080)
    operations = [
        {'operation': 'delete', 'key': 'bbbb', 'value': 'BBBB'},
    ]
    client.patch(operations, 8080)

    operations = [
        {'operation': 'add', 'key': 'qqqq', 'value': 'QqQq'},
    ]
    client.patch(operations, 8082)
    operations = [
        {'operation': 'update', 'key': 'mmmm', 'value': 'MmMm'},
    ]
    client.patch(operations, 8082)

    operations = [
        {'operation': 'delete', 'key': 'tttt', 'value': 'TTTT'},
    ]
    client.patch(operations, 8084)
    time.sleep(0.5)
    print(f'Node 1 {nodes[0].get_data_storage()}')
    print(f'Node 2 {nodes[1].get_data_storage()}')
    print(f'Node 3 {nodes[2].get_data_storage()}')
    print(f'Node 4 {nodes[3].get_data_storage()}')
    print(f'Node 5 {nodes[4].get_data_storage()}')

    wait_consistency(nodes)
    d1 = nodes[0].get_data_storage()
    d2 = nodes[1].get_data_storage()
    d3 = nodes[2].get_data_storage()
    d4 = nodes[3].get_data_storage()
    d5 = nodes[4].get_data_storage()
    time.sleep(0.5)
    assert d1 == d2 == d3 == d4 == d5
    print(f'All consistent: common data = {d1}')
    assert d1 == {'aaaa': 'aAaA', 'mmmm': 'MmMm', 'qqqq': 'QqQq'}
    print('Test 7 passed.')

test_8()