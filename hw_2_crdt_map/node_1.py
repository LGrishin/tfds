import threading
import zmq
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import time
from client import Client
from vector_clock import independentLists, mergeVectors, lessEqList
import random

def get_msg_unique_str_key(msg):
    msg_sender_id = msg['sender_id']
    msg_number = msg['deps'][msg_sender_id]
    return f"{msg_sender_id}, {msg_number}"

class Node:
    def __init__(self, node_id, nodes_count, random_crash=False):
        self.http_port = 8080 + node_id
        self.zmq_port = 5555 + node_id
        
        self.random_crash = random_crash
        
        self.node_id = node_id
        self.nodes_count = nodes_count
        self.timestamps = []
        
        self.data_store = {}
        self.lock = threading.Lock()  # protect broadcast
                
        # Vector clock
        self.clock = [0 for _ in range(self.nodes_count)]
        self.clock[self.node_id] = 1
        
        # causal broadcast
        self.delivered = [0 for _ in range(self.nodes_count)]
        self.delivered_msg = set()
        self.buffer = set()
        self.sended_count = 0

        # communication
        self.context = zmq.Context()
        
        self.zmq_recive_socket = self.context.socket(zmq.PULL)
        self.zmq_recive_socket.bind(f"tcp://*:{self.zmq_port}")
        
        # nodes msg processing
        self.zmq_thread = threading.Thread(target=self.handle_zmq_messages)
        self.zmq_thread.daemon = True # aka .join()
        self.zmq_thread.start()

        # asyn communication with user
        self.http_thread = threading.Thread(target=self.run_http_server)
        self.http_thread.daemon = True 
        self.http_thread.start()
    
    def get_data_storage(self):
        result = {}
        for key, value in self.data_store.items():
            result[key] = value['data']
        return result
    
    def new_timestamp(self):
        self.clock[self.node_id] += 1
    
    def isolate(self, timeout):
        msg = {
            'msg_type': 'sleep',
            'sleep_time': timeout
        }
        self.send_message(msg, self.node_id)
        
    def send_message(self, message, node_id):
        if self.random_crash:
            timeout = random.uniform(0.2, 0.3)
            time.sleep(timeout)
        sender_socket = self.context.socket(zmq.PUSH)
        sender_socket.connect(f"tcp://localhost:{5555 + node_id}")
        sender_socket.send_json(message)
        
    def handle_zmq_messages(self):
        while True:
            try:
                if self.random_crash:
                    socket_timeout = random.randint(200, 300)
                    self.zmq_recive_socket.setsockopt(zmq.RCVTIMEO, socket_timeout)

                message = self.zmq_recive_socket.recv_json()

                msg_type = message['msg_type']
                if msg_type == 'sleep':
                    print(f'Node {self.node_id} is isolated')
                    time.sleep(message['sleep_time'])
                    print(f'Node {self.node_id} already active')
                    
                elif msg_type == 'broadcast':
                    if not self.already_delivered(message):
                        self.causal_broadcast_process_message(message)
                        self.broadcast(message)
                        
                elif msg_type == 'new_message':
                    message['msg_type'] = 'broadcast'
                    # print(f'Node {self.node_id} SENDING {message}')
                    self.broadcast(message)
            except zmq.Again:
                if self.random_crash:
                    timeout = random.uniform(0.2, 0.3)
                    # print(f'Node {self.node_id} is isolated by random timer')                    
                    time.sleep(timeout)
                    # print(f'Node {self.node_id} already active (random timer)')
                    
    def already_delivered(self, message):
        msg_key = get_msg_unique_str_key(message)
        json_msg = json.dumps(message)
        with self.lock:
            if msg_key in self.delivered_msg or json_msg in self.buffer:
                assert(message['msg_type'] != 'new_message')
                return True
        return False

    def find_next_message(self):
        result = None
        
        for str_msg in self.buffer:
            msg = json.loads(str_msg)
            if lessEqList(msg['deps'], self.delivered):
                result = msg
                break
            # else:
            #     print(msg['deps'], self.delivered)
            
        if result is not None:
            str_res = json.dumps(result)
            self.buffer.remove(str_res)
            
        return result
    
    def causal_broadcast_process_message(self, message):
        # assert(self.already_delivered(message) == False)
        str_message = json.dumps(message)
        assert(str_message not in self.buffer)
        # print(f'Node {self.node_id}, del = {self.delivered} recive msg {message["deps"]}')
        self.buffer.add(str_message)
        next_msg = self.find_next_message()
        
        response = None
            
        while next_msg is not None:
            if message['msg_type'] == 'new_message':
                assert(response is None)
                response = self.process_message(next_msg)
            else:
                self.process_message(next_msg)

            self.delivered[next_msg['sender_id']] += 1
            # print(f'Node {self.node_id} current data store state is {self.data_store}, deps {self.delivered}')
            next_msg = self.find_next_message()
        return response
    
    # возвращаем True если применяем запрос 1
    def resolve_conflict(self, request, curr_data):
        new_timestamp = request['timestamp']
        curr_timestamp = curr_data['timestamp']
        if lessEqList(new_timestamp, curr_timestamp):
            return False
        if lessEqList(curr_timestamp, new_timestamp):
            return True
        new_sender_id = request['sender_id']
        old_sender_id = curr_data['sender_id']
        if new_sender_id < old_sender_id:
            return True
        return False
        
    def process_message(self, message):
        response_messages = []
        
        self.delivered_msg.add(get_msg_unique_str_key(message))
        # print(f'Node {self.node_id} process message : {message}')
        
        # msg_clock = VectorClock.message['timestamp'])
        msg_clock = message['timestamp']
        assert(msg_clock not in self.timestamps)

        self.timestamps.append(msg_clock)
        self.clock = mergeVectors(self.clock, msg_clock)
        
        if message['sender_id'] != self.node_id: # for new messages
            self.new_timestamp()

        # print(f'Node {self.node_id} clock after: {self.clock}')

        for operation in message['operations']:
            action = operation.get("operation", "update")
            key = operation.get("key")
            new_value = operation.get("value", '')

            if action == "update":
                if key in self.data_store:
                    new_request = {'timestamp': message['timestamp'], 'sender_id': message['sender_id']}
                    curr_state = {'timestamp': self.data_store[key]['timestamp'], 'sender_id': self.data_store[key]['sender_id']}
                    if self.resolve_conflict(new_request, curr_state):
                        self.data_store[key] = {'data': new_value, 'timestamp': message['timestamp'], 'sender_id': message['sender_id']}
                        response_messages.append({"message": "Value updated", "key": key, "new_value": new_value})
                    
                else:
                    response_messages.append({"message": "Key not found", "key": key})

            elif action == "delete":
                if key in self.data_store:
                    new_request = {'timestamp': message['timestamp'], 'sender_id': message['sender_id']}
                    curr_state = {'timestamp': self.data_store[key]['timestamp'], 'sender_id': self.data_store[key]['sender_id']}
                    if self.resolve_conflict(new_request, curr_state):
                        del self.data_store[key]
                        response_messages.append({"message": "Key deleted", "key": key})
                    else:
                        response_messages.append({"message": "Ignored"})

                else:
                    response_messages.append({"message": "Key not found", "key": key})

            elif action == "add":
                if key not in self.data_store:
                    self.data_store[key] = {'data': new_value, 'timestamp': message['timestamp'], 'sender_id': message['sender_id']}
                    response_messages.append({"message": "Key added", "key": key, "new_value": new_value})
                else:
                    new_request = {'timestamp': message['timestamp'], 'sender_id': message['sender_id']}
                    curr_state = {'timestamp': self.data_store[key]['timestamp'], 'sender_id': self.data_store[key]['sender_id']}
                    if self.resolve_conflict(new_request, curr_state):
                        self.data_store[key] = {'data': new_value, 'timestamp': message['timestamp'], 'sender_id': message['sender_id']}
                        response_messages.append({"message": "Key updated", "key": key, "new_value": new_value})
                    else:
                        response_messages.append({"message": "Ignored", "key": key})
        # print(f'Node {self.node_id} current data store state is {self.data_store}, clock {self.clock}')
        return response_messages
    
    def broadcast(self, operations):                
        for node_id in range(self.nodes_count):
            self.send_message(operations, node_id)
    
    def send(self, message):
        with self.lock:
            self.new_timestamp()
            message['timestamp'] = self.clock

            message['msg_type'] = 'new_message'
            message['sender_id'] = self.node_id
            deps = self.delivered
            deps[self.node_id] = self.sended_count
            message['deps'] = deps.copy()
            response = self.causal_broadcast_process_message(message)
            self.send_message(message, self.node_id)
            self.sended_count += 1
            return response

    def run_http_server(self):
        # Запускаем HTTP сервер
        self.http_server = HTTPServer(('localhost', self.http_port), lambda *args: self.RequestHandler(self, *args))
        print(f"HTTP Server running on port {self.http_port}")
        self.http_server.serve_forever()

    class RequestHandler(BaseHTTPRequestHandler):
        def __init__(self, node_instance, *args):
            self.node_instance = node_instance  # parent Node
            super().__init__(*args)

        def do_GET(self):
            key = self.path[1:]  # get key
            with self.node_instance.lock:
                value = self.node_instance.data_store.get(key, "Key not found")
                value = value['data']
                
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = json.dumps({"key": key, "value": value})
            self.wfile.write(response.encode())

        def do_PATCH(self):
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            try:
                operations = json.loads(post_data)  # try convert to json
                operations_data = operations['operations']
                if not isinstance(operations_data, list):
                    raise ValueError("Operations must be a list")
            except (json.JSONDecodeError, ValueError) as e:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(json.dumps({"message": str(e)}).encode())
                return

            
            response_messages = self.node_instance.send(operations)

            response = {'data': response_messages}

            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())


def main():
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
    
    for node_id in range(len(nodes)):
        print(f'Node {node_id} data store is {nodes[node_id].data_store}')
    time.sleep(3)
    for node_id in range(len(nodes)):
        print(f'Node {node_id} data store is {nodes[node_id].data_store}')
    print(client.get('aaa', 8081))
    time.sleep(1)
# if __name__ == "__main__":
#     main()
