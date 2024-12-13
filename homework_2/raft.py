import zmq
import threading

class Node:
    def __init__(self, node_id, port, nodes_count):
        self.node_id = node_id
        self.port = port
        self.data_store = {}
        self.nodes_count = nodes_count
        self.context = zmq.Context()
        self.msg_count = 0
        # консенсус
        self.type = 'follower'
        self.era = 0
        self.voited_in_this_era = False
        self.leader_id = None
        self.voites_recived = set()
        self.sendLen = []
        self.voited_for = None

        self.quorum = ((nodes_count + 1) // 2) + ((nodes_count + 1) % 2)
        self.election_timeout = (1 + node_id * 2) * 1000
        self.heartbeat_timeout = 800
        self.heartbeat_waiting_timeout = 100
        self.voite_response_timeout = 100

        # лог и все что с ним связано
        self.log = []
        self.commit_length = 0
        self.sent_length = [0 for _ in range(self.nodes_count)]
        self.acked_length = [0 for _ in range(self.nodes_count)]
        
    def start(self):
        print('Starting node {}'.format(self.node_id))
        
        # подъем после краша
        self.socket = self.context.socket(zmq.PULL)
        self.socket.bind(f"tcp://*:{self.port}")
        self.running = True
        self.type = 'follower'
        self.leader_id = None
        
        thread = threading.Thread(target=self.run)
        thread.start()

    def run(self):
        while self.running:
            try:
                # print(f'Node {self.node_id} is waiting...')
                socket_timeout = self.election_timeout
                if self.type == 'leader':
                    socket_timeout = self.heartbeat_timeout
                    
                self.socket.setsockopt(zmq.RCVTIMEO, socket_timeout)
                message = self.socket.recv_json()
                msg_type = message.get("msg_type")
                
                if msg_type == "STOP":
                    print(f"Node {self.node_id} CRASHED")
                    break

                elif msg_type == "user_request":
                    self.process_user_request(message)
                    
                elif msg_type == "voite_request":
                    self.process_voite_request(message)

                elif msg_type == 'voite_response':
                    self.process_voite_response(message)
                    
                elif msg_type == 'log_request':
                    self.process_log_request(message)
                    
                elif msg_type == 'log_response':
                    self.process_log_response(message)
                
                print(f'Node {self.node_id}, log len = {len(self.log)}, curr data is {self.data_store}')
                # print(f'Node {self.node_id}, log {self.log}')
            except zmq.Again:
                if self.type == 'follower':
                    self.election()
                if self.type == 'leader':
                    self.broadcast_replicate_log()
        self.socket.close()
    
    def send_message(self, message, port):
        node_id = port - 5555
        try:
            socket = self.context.socket(zmq.PUSH)
            socket.connect(f"tcp://localhost:{port}")
            socket.send_json(message)
        except Exception as e:
            print(f"Error communicating with node {node_id}: {e}")
    
    def process_user_request(self, message):
        if message['operation_type'] == 'ping':
            user_response = {
                'msg_type': 'user_response',
                'response_type': 'ping',
                'msg_id': message['msg_id'],
            }
            self.send_message(user_response, message['user_port'])
            return
        
        msg_otype = message['operation_type']
        
        if msg_otype == 'push' or msg_otype == 'delete':
            if self.type == 'leader':
                message['era'] = self.era
                self.log.append(message)
                self.acked_length[self.node_id] = len(self.log)
                print(f'Node {self.node_id} ({self.type}) recive user request msg = {message})')
                self.broadcast_replicate_log()
            elif self.type == 'follower' and self.leader_id is not None:
                self.send_message(message, self.leader_id + 5555)
                
        elif msg_otype == 'put':
            if self.type == 'leader':
                message['era'] = self.era
                key = message['key']
                                
                if key not in self.data_store:
                    return
                
                self.log.append(message)
                self.acked_length[self.node_id] = len(self.log)
                # print(f'Node {self.node_id} ({self.type}) recive user request msg = {message})')
                self.broadcast_replicate_log()
            elif self.type == 'follower' and self.leader_id is not None:
                self.send_message(message, self.leader_id + 5555)
                
        elif msg_otype == 'get':
            if self.type == 'leader':
                self.msg_count += 1
                message['redirected'] = True
                dst_id = None
                dst_ids = []
                for node_id in range(self.nodes_count):
                    if node_id == self.node_id or self.commit_length != self.sent_length[node_id]:
                        continue
                    dst_ids.append(node_id)
                
                # print(f'good nodes is {dst_ids}')
                
                if len(dst_ids) > 0:
                    dst_id = self.msg_count % len(dst_ids)
                if dst_id is not None:
                    self.send_message(message, dst_id + 5555)
                    return
                
            if message['redirected'] == False:
                self.send_message(message, self.leader_id + 5555)
                return
            
            # print(f'Node {self.node_id} recive final user get request')
            user_response = {
                'msg_type': 'user_response',
                'response_type': 'get',
                'msg_id': message['msg_id'],
                'value': None
            }
            key = message['key']
            # print(f'Node {self.node_id} recive user_request {key}, stored data: {self.data_store}')

            if key in self.data_store:
                user_response['value'] = self.data_store[key]
            self.send_message(user_response, message['user_port'])
            
    def process_voite_response(self, message):
        # print(f'Node {self.node_id} ({self.type}) recive voite response')
        
        if self.type == 'candidate':
            if message['era'] > self.era:
                self.era = message['era']
                self.type ='follower'
                self.voited_for = None
            if message['voite'] == True:
                self.voites_recived.add(message['node_id'])
            if len(self.voites_recived) >= self.quorum:
                print(f'Node {self.node_id} (Leader) is election winner')
                self.type = 'leader'
                self.leader_id = self.node_id
            
    def process_voite_request(self, message):
        my_log_era = (self.log[-1]['era'] if len(self.log) > 0 else 0)
        log_ok = (my_log_era < message['log_era'] or
                     (my_log_era == message['log_era'] and len(self.log) <= message['log_len']))
        era_ok = (message['era'] > self.era or
                      (message['log_era'] == self.era and self.voited_for in [None, message['node_id']]))

        voite_message = {
            'msg_type': 'voite_response',
            'era': self.era,
            'node_id': self.node_id,
        }

        port = 5555 + message['node_id']
        
        if log_ok and era_ok:
            self.era = message['era']
            self.type = 'follower'
            self.voited_for = {message['node_id']}
            voite_message['voite'] = True
            self.voited_for = message['node_id']
        else:
            voite_message['voite'] = False
        
        # print(f'Node {self.node_id} recive voite request from {message["node_id"]}, voite is {voite_message["voite"]}')
        
        self.send_message(voite_message, port)

    def election(self):
        print(f"Node {self.node_id} initialize the leader election")
        self.era += 1
        log_era = (self.log[-1]['era'] if len(self.log) > 0 else 0)
        self.type = 'candidate'
        self.voites_recived = set()
        self.voites_recived.add(self.node_id)
        
        voite_request = {'era': self.era,
                         'node_id': self.node_id,
                         'msg_type': 'voite_request',
                         'log_era': log_era,
                         'log_len': len(self.log)
                         }

        for other_node_id in range(self.nodes_count):
            other_node_port = 5555 + other_node_id
            if other_node_id != self.node_id:
                self.send_message(voite_request, other_node_port)


    def broadcast_replicate_log(self):
        assert(self.type == 'leader')
        # print(f'Node {self.node_id} ({self.type}) starting log replication')
        for followerId in range(self.nodes_count):
            if followerId != self.node_id:
                self.replicate_log(self.node_id, followerId)

    def replicate_log(self, leaderId, followerId):
        i = self.sent_length[followerId]
        entries = self.log[i:]
        log_era = 0
        if i > 0:
            log_era = self.log[-1]['era']
        log_request = {
            'msg_type': 'log_request',
            'leader_id': self.node_id,
            'era': self.era,
            'prefix_log_len': i,
            'log_era': log_era,
            'commited': self.commit_length,
            'entries': entries
        }
        
        self.send_message(log_request, followerId + 5555)

    def process_log_request(self, message):
        # print(f'Node {self.node_id} recive lor request from {message["leader_id"]} leader era {message["era"]} my era {self.era}')
        if self.era <= message['era']:
            self.era = message['era']
            self.voited_for = None
            self.type = 'follower'
            self.leader_id = message['leader_id']
        if self.era == message['era'] and self.type == 'candidate':
            self.type = 'follower'
            self.leader_id = message['leader_id']
        if self.leader_id is None:
            self.leader_id = message['leader_id']
        
        prefix_log_len = message['prefix_log_len']
        log_ok = (len(self.log) >= prefix_log_len) and (prefix_log_len == 0 or message['log_era'] == self.log[-1]['era'])

        ack = 0
        success = False
        # print(f'Node {self.node_id} recive log request, log_ok {log_ok}, entries {message["entries"]}')
        if self.era == message['era'] and log_ok:
            self.append_entries(prefix_log_len, message['commited'], message['entries'])
            ack = prefix_log_len + len(message['entries'])
            success = True

        log_response = {
            'msg_type': 'log_response',
            'era': self.era,
            'ack': ack,
            'success': success,
            'node_id': self.node_id,
        }
    
        self.send_message(log_response, self.leader_id + 5555)
    
    def process_log_response(self, message):
        message_era = message['era']
        ack = message['ack']
        success = message['success']
        follower_id = message['node_id']
        # print(f'Node {self.node_id} recive log response from node {follower_id} with ack {ack} and {success}')
        if self.era == message_era and self.type == 'leader':
            if success == True and ack >= self.acked_length[follower_id]:
                self.sent_length[follower_id] = ack
                self.acked_length[follower_id] = ack
                self.commit_log_entries()
            elif self.sent_length[follower_id] > 0:
                self.sent_length[follower_id] = self.sent_length[follower_id] - 1
                self.replicate_log(self.node_id, follower_id)
        elif self.era < message_era:
            self.era = message_era
            self.type = 'follower'
            self.voited_for = None
        # print(f'Node {self.node_id} recive log response, acks = {self.acked_length}')
    
    def acks_count(self, length):
        count = 0
        for node_id in range(self.nodes_count):
            if self.acked_length[node_id] >= length:
                count += 1
        return count
    
    def commit_log_entries(self):
        ready = None
        for i in range(len(self.log)):
            if self.acks_count(i + 1) >= self.quorum:
                if ready is None:
                    ready = i + 1
                else:
                    ready = max(ready, i + 1)
        # print(f'Node {self.node_id} ready is {ready} , acked {self.acked_length} , self era = {self.era}')
        if ready is not None and ready > self.commit_length and self.log[ready - 1]['era'] == self.era:
            for i in range(self.commit_length, ready):
                self.execute_entry(self.log[i])
            self.commit_length = ready
            
    def append_entries(self, prefix_log_len, commited_len, entries):
        if len(entries) > 0 and len(self.log) > prefix_log_len:
            if self.log[-1]['era'] != entries[0]['era']:
                self.log = self.log[:prefix_log_len - 1]

        if prefix_log_len + len(entries) > len(self.log):
            # self.log.extend(entries)
            for i in range(len(self.log) - prefix_log_len, len(entries)):
                self.log.append(entries[i])
        
        if commited_len > self.commit_length:
            for i in range(self.commit_length, commited_len):
                self.execute_entry(self.log[i])
            self.commit_length = commited_len
        
    def execute_entry(self, message):
        key = message['key']
        if message['operation_type'] == 'push':
            value = message['value']
            self.data_store[key] = value
        elif message['operation_type'] == 'put':
            assert(key in self.data_store)
            value = message['value']
            self.data_store[key] = value
        elif message['operation_type'] == 'delete':
            if key in self.data_store:
                del self.data_store[key]
            else:
                print('ERROR key does not exist!!')
                
    def stop(self):
        self.running = False
        socket = self.context.socket(zmq.PUSH)
        socket.connect(f"tcp://localhost:{self.port}")
        socket.send_json({'msg_type': 'STOP'})
        socket.close()

