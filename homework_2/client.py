from raft import Node
import zmq

class Client:
    def __init__(self, nodes):
        self.nodes = nodes
        self.ping_timeout = 100
        self.context = zmq.Context()
        self.port = 5500
        self.msg_id = 0
        self.pull_socket = self.context.socket(zmq.PULL)
        
        self.pull_socket.bind(f"tcp://*:{self.port}")
        self.pull_socket.setsockopt(zmq.RCVTIMEO, self.ping_timeout)

    def send_message(self, message, port):
        try:
            socket = self.context.socket(zmq.PUSH)
            socket.connect(f"tcp://localhost:{port}")
            socket.send_json(message)
            socket.close()
        except Exception as e:
            print(f"Error communicating with port {port}: {e}")

    def node_is_alive(self, port):
        self.msg_id += 1
        
        message = {
            'msg_type': 'user_request',
            'operation_type': 'ping',
            'user_port': self.port,
            'msg_id': self.msg_id
        }
        
        self.send_message(message, port)
        while True:
            try:
                response = self.pull_socket.recv_json()
                if response['msg_type'] == 'user_response' and response['response_type'] == 'ping' and response['msg_id'] == self.msg_id:
                    return True
            except zmq.Again:
                return False
    

    def add_value(self, key, value):
        message = {
            'msg_type': 'user_request',
            'key': key,
            'value': value,
            'operation_type': 'push',
            'user_port': self.port,
        }
        
        for node_id in range(len(self.nodes)):
            node_port = node_id + 5555
            if self.node_is_alive(node_port):
                self.send_message(message, node_port)
                break
            
    def try_get_value(self, key, port):
        self.msg_id += 1
        
        message = {
            'msg_type': 'user_request',
            'operation_type': 'get',
            'redirected': False,
            'key': key,
            'user_port': self.port,
            'msg_id': self.msg_id,
        }
        
        self.send_message(message, port)
        while True:
            try:
                response = self.pull_socket.recv_json()
                if response['msg_type'] == 'user_response' and response['response_type'] == 'get' and response['msg_id'] == self.msg_id:
                    return response['value']
            except zmq.Again:
                return None

    def get_value(self, key):
        self.msg_id += 1
        print('USER GET VALUE')
        for node_id in range(len(self.nodes)):
            node_port = node_id + 5555
            value = self.try_get_value(key, node_port)
            if value != None:
                return value
        return None
    
    def delete_value(self, key):
        self.msg_id += 1
        
        message = {
            'msg_type': 'user_request',
            'key': key,
            'operation_type': 'delete',
            'user_port': self.port,
            'msg_id': self.msg_id
        }
        
        for node_id in range(len(self.nodes)):
            node_port = node_id + 5555
            if self.node_is_alive(node_port):
                self.send_message(message, node_port)
                break
            