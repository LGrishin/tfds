from node_2 import Node


import time
def nodes_data_consistent(data):
    assert len(data) > 0
    first = data[0]
    for i in range(len(data)):
        if first != data[i]:
            return False
    return True

def wait_consistency(nodes):
    while True:
        data = []
        for i in range(len(nodes)):
            data.append(nodes[i].get_data_storage())

        if nodes_data_consistent(data):
            return

        time.sleep(0.5)
