import zmq
import threading
import time

class Message:
    def __init__(self, key, value, msgType):
        self.key = key
        self.value = value
        self.msgType = msgType
    
    def __str__(self):
        return f"Message(key='{self.key}', value='{self.value}', type='{self.msgType}')"
    
class Node:
    def __init__(self, node_id, port, nodes_count):
        self.node_id = node_id
        self.port = port
        self.data_store = {}
        self.nodes_count = nodes_count
        self.context = zmq.Context()

        # консенсус
        self.type = 'follower'
        self.election_timeout = (1 + node_id * 2) * 1000
        self.last_heartbeat = time.time()
        self.era = 0
        self.voited_in_this_era = False
        self.heartbeat_timeout = 800
        self.leader_id = None
        
        self.heartbeat_waiting_timeout = 100
        self.voite_response_timeout = 100
        
        # обработка сообщений
        self.alive_nodes = [True for _ in range(nodes_count)]
        
        # лог и все что с ним связано
        self.log = []
        self.commit_length = 0
        
    def start(self):
        print('Starting node {}'.format(self.node_id))
        
        # подъем после краша
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{self.port}")
        self.running = True
        self.type = 'follower'
        self.leader_id = None
        
        thread = threading.Thread(target=self.run)
        thread.start()

    def run(self):
        while self.running:
            try:
                socket_timeout = self.election_timeout
                if self.type == 'leader':
                    socket_timeout = self.heartbeat_timeout
                    
                self.socket.setsockopt(zmq.RCVTIMEO, socket_timeout)
                message = self.socket.recv_json()
                msg_type = message.get("msg_type")
                
                # print(f"Node {self.node_id} recive msg_type: {msg_type}")
                
                if msg_type == 'user_request':
                    response = self.process_user_msg(message)
                    self.socket.send_json(response)

                elif msg_type == "STOP":
                    print(f"Node {self.node_id} CRASHED")
                    break
                
                elif msg_type == "election":
                    response = self.process_election_request(message)
                    self.socket.send_json(response)
                    
                elif msg_type == "heartbeat":
                    response = self.process_heartbeat_msg(message)
                    self.socket.send_json(response)
                
                else:
                    self.socket.send_json({"error": "Unknown msg_type"})
                    
            except zmq.Again:
                if self.type == 'follower':
                    self.election()
                if self.type == 'leader':
                    self.heartbeat_broadcast()
                # break

        self.socket.close()
    
    def update_log(self, new_data):
        for entry in new_data:
            self.log.append(entry)
            if entry['request_type'] == 'push':
                self.data_store[entry['key']] = entry['value']
            else:
                assert(False)   # unknow operation, тут можно поддержать удаление условно
        self.commit_length = len(self.log)
        
    def process_user_msg(self, message):
        if message['request_type'] == 'push':
            self.add_data(message['key'], message['value'], broadcast=False)
            # print(f'Node {self.node_id} recived user data {message}')
            return {'status': 'OK'}
        elif message['request_type'] == 'get':
            return {'status': 'OK', 'value': self.get_data(message['key'])}
        elif message['request_type'] == 'get_leader':
            # возможно надо добавить обработку на случай когда идет процесс голосования
            return {'status': 'OK', 'leader_id': self.leader_id}
        print('UNKNOW USER MSG TYPE ', message['request_type']) 
        
    def process_election_request(self, message):
        response = {} # обработать случай с логом
        if self.era < message['era'] or (self.era == message['era'] and self.voited_in_this_era == False):
            self.era = message['era']
            self.voited_in_this_era = True
            response = {'msg_type': 'voite', 'value': True, 'era': self.era}
        else:
            response = {'msg_type': 'voite', 'value': False, 'era': self.era}
        return response

        

    def add_data(self, key, value, broadcast=True):
        assert(self.node_id == self.leader_id)  # Only leader can add data
        
        print('Add data to node {} (Leader)'.format(self.node_id))
        message = {"msg_type": 'user_request',
                   'request_type' : 'push',
                   "key": key, "value": value,
                   'era': self.era, 'ack_count': 0}

        self.log.append(message)
        print(f"Node {self.node_id} (Leader) logged message: {key} -> {value}")
        self.commit_length += 1 # Todo: это должно происходить только после репликации
        self.data_store[key] = value
        

    def get_data(self, key):
        print('Get data from node {}'.format(self.node_id))
        return self.data_store.get(key, None)

    def send_message(self, message, port, timeout=None):
        node_id = port - 5555
        try:
            socket = self.context.socket(zmq.REQ)
            
            if timeout is not None:
                socket.setsockopt(zmq.RCVTIMEO, timeout)

            socket.connect(f"tcp://localhost:{port}")
            socket.send_json(message)
            try:
                response = socket.recv_json()  # Ожидаем ответ
                return response
            except zmq.Again:
                print(f'Node {self.node_id} (Leader): node {node_id} is dead')
        except Exception as e:
            print(f"Error communicating with node {node_id}: {e}")
        return None
    
    def heartbeat_broadcast(self):
        print('Node {} (Leader) starting heartbeat broadcast'.format(self.node_id))
        
        for other_node_id in range(self.nodes_count):
            other_node_port = 5555 + other_node_id
            if other_node_id != self.node_id:
                heartbeat_message = {'msg_type': 'heartbeat', 'era': self.era, 'leader_id': self.node_id, 'phase': 'first'}
                response = self.send_message(heartbeat_message, other_node_port, self.heartbeat_waiting_timeout)
                if response is None:
                    self.alive_nodes[other_node_id] = False
                    continue
                if response['status'] == 'OK':
                    self.alive_nodes[other_node_id] = True
                    node_commit_lenght = response['commit_length']
                    if self.commit_length > node_commit_lenght:
                        heartbeat_message['data'] = self.log[node_commit_lenght:self.commit_length + 1]
                        heartbeat_message['phase'] = 'second'
                        response = self.send_message(heartbeat_message, other_node_port, self.heartbeat_waiting_timeout)
                        # print('-----> Leader send data:', heartbeat_message['data'])
                        if response is not None and response['status'] == 'OK':
                            for entry in self.log[node_commit_lenght::self.commit_length + 1]:
                                entry['ack_count'] += 1
        
        alive_id = []
        for other_node_id in range(self.nodes_count):
            if self.alive_nodes[other_node_id] == True:
                alive_id.append(other_node_id)
        
        print(f'Node {self.node_id} (Leader): alive nodes is {alive_id}')

    def process_heartbeat_msg(self, message):
        # print(f'Node {self.node_id} recive hartbeat: {message}')
        if self.era < message['era']:
            self.era = message['era']
            self.leader_id = message['leader_id']
            self.type = 'follower'
        if message['phase'] == 'first':
            return {'msg_type': 'heartbeat_response', 'status': 'OK', 'commit_length': self.commit_length}
        if message['phase'] == 'second':
            new_data = message['data']
            # print(f'Node {self.node_id} starting log update, new data: {new_data}')
            self.update_log(new_data)
            # print(f'Node {self.node_id}: log successfly updated: {self.log}')
            print(f'Node {self.node_id}: data updated: {self.data_store}')
            
            return {'msg_type': 'heartbeat_response', 'status': 'OK'}
        assert(False)

    def election(self):
        print(f"Node {self.node_id} initializes the leader election")
        self.era += 1
        self.type = 'candidate'
        stop_election = False
        alive_nodes_count = 0
        voites = 0

        message = {'era': self.era, 'leader': self.node_id, 'msg_type': 'election'}

        for other_node_id in range(self.nodes_count):
            other_node_port = 5555 + other_node_id
            if other_node_id != self.node_id:
                try:
                    socket = self.context.socket(zmq.REQ)
                    socket.setsockopt(zmq.RCVTIMEO, self.voite_response_timeout)
                    socket.connect(f"tcp://localhost:{other_node_port}")
                    socket.send_json(message)
                    print('Node {} waiting response from node {}'.format(self.node_id, other_node_id))
                    try:
                        response = socket.recv_json() 
                        if response['msg_type'] == 'voite':
                            if response['value'] == True:
                                voites += 1
                            if response['era'] > self.era:
                                stop_election = True
                                print('Node {} election failed, new era exist'.format(self.node_id))
                                break
                            alive_nodes_count += 1
                        else:
                            print(f"ERROR: response type from node {other_node_id} during election is {response['msg_type']}")
                    except zmq.Again:
                        print(f'Node {self.node_id} election timeout exceeded, node {other_node_id} is dead')
                except Exception as e:
                    print(f"Error communicating with node (election) {other_node_id}: {e}")

        if voites > alive_nodes_count / 2 and stop_election == False: # todo: change alive count to nodes count
            print(f'Node {self.node_id}: I am election winner!!!!')
            self.type = 'leader'
            self.leader_id = self.node_id
            return

        self.type = 'follower'


    def broadcast(self, message):
        # time.sleep(3)
        print('Node {} starting broadcast'.format(self.node_id))
        for other_node_id in range(self.nodes_count):
            other_node_port = 5555 + other_node_id
            if other_node_id != self.node_id:
                try:
                    socket = self.context.socket(zmq.REQ)
                    socket.connect(f"tcp://localhost:{other_node_port}")
                    socket.send_json(message)
                    print('Node {} waiting response from node {}'.format(self.node_id, other_node_id))
                    response = socket.recv_json()  # Ожидаем ответ
                    print('Node {} received response from node {}'.format(self.node_id, other_node_id))
                except Exception as e:
                    print(f"Error communicating with node {other_node_id}: {e}")

    def stop(self):
        self.running = False
        socket = self.context.socket(zmq.REQ)
        socket.connect(f"tcp://localhost:{self.port}")
        socket.send_json({'msg_type': 'STOP'})
        socket.close()


class Client:
    def __init__(self, nodes):
        self.nodes_ = nodes
        self.waiting_timeout = 300
    
    def getLeaderId(self):
        for node in self.nodes_:
            socket = zmq.Context().socket(zmq.REQ)
            socket.setsockopt(zmq.RCVTIMEO, self.waiting_timeout)        
            socket.connect(f"tcp://localhost:{node.port}")
            socket.send_json({'msg_type': 'user_request', 'request_type': 'get_leader'})
            try:
                response = socket.recv_json()  # Ожидаем ответ
                return response['leader_id']
            except zmq.Again:
                continue
        return None

    def push(self, key, value):
        leader_id = self.getLeaderId()
        if leader_id is None:
            print('Client: cluster crashed')
            return
        self.nodes_[leader_id].add_data(key, value)
        # socket = zmq.Context().socket(zmq.REQ)
        # socket.connect(f"tcp://localhost:{self.nodes_[leader_id].port}")
        # socket.send_json({'msg_type': 'user_request', 'request_type': 'push', 'key': key, 'value': value})
        # socket.close()

    def get(self, key):
        leader_id = self.getLeaderId()
        if leader_id is None:
            print('Client: cluster crashed')
        # socket = zmq.Context().socket(zmq.REQ)
        # socket.connect(f"tcp://localhost:{self.nodes_[leader_id].port}")
        # socket.send_json({'msg_type': 'user_request', 'request_type': 'get', 'key': key})
        # response = socket.recv_json()
        # return response['value']
        return self.nodes_[leader_id].get_data(key)

# Создаем узлы
nodes = []
num_nodes = 3  # Количество узлов
for i in range(num_nodes):
    node = Node(node_id=i, port=5555 + i, nodes_count=num_nodes)
    nodes.append(node)
    node.start()

time.sleep(7)
nodes[0].stop()
time.sleep(7)
nodes[0].start()
time.sleep(7)

client = Client(nodes)

client.push('НИС', '0')
client.push('ТОРС', '1')
client.push('БЛОКЧЕЙН', '2')
client.push('ПСИХОЛОГИЯ', '4')
# print("----> Leader LOG ", nodes[1].log)
time.sleep(7)
nodes[2].stop()
time.sleep(7)
client.push('AAA', 'aaa')
client.push('BBB', 'bbb')
client.push('CCC', 'ccc')
time.sleep(7)
nodes[2].start()
time.sleep(7)

print(f"Value: {client.get('ПСИХОЛОГИЯ')}")

# Остановка узлов (по желанию)
for node in nodes:
    node.stop()
