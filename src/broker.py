#!/usr/bin/env python3

import socket
import threading
import struct
import sys
import traceback
import time

class MQTTBroker:
    def __init__(self, host='0.0.0.0', port=1883):
        # Initialize the broker with host and port
        self.host = host
        self.port = port
        # Map client sockets to client IDs
        self.clients = {}
        # Store session data per client ID
        self.client_sessions = {}
        # Map client sockets to their subscribed topics
        self.topic_subscribers = {}
        # Store retained messages per topic
        self.retained_messages = {}
        # Lock for thread safety
        self.lock = threading.Lock()

    def start(self):
        # Start the MQTT broker server
        try:
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Allow reuse of local addresses
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind((self.host, self.port))
            server_sock.listen(5)
            print(f"MQTT Broker listening on {self.host}:{self.port}")

            while True:
                # Accept incoming client connections
                client_sock, addr = server_sock.accept()
                print(f"Client connected from {addr}")
                # Handle each client in a new thread
                threading.Thread(target=self.handle_client, args=(client_sock,)).start()
        except Exception as e:
            print(f"Error starting the broker: {e}")
            traceback.print_exc()
        finally:
            server_sock.close()
            print("Broker stopped.")

    def handle_client(self, client_sock):
        # Handle communication with a connected client
        client_id = None
        clean_session = True
        keep_alive = None
        last_packet_time = time.time()
        try:
            # Receive CONNECT packet
            packet = self.receive_packet(client_sock)
            if not packet:
                return
            packet_type = packet[0] >> 4
            if packet_type != 1:
                print("Expected CONNECT packet")
                return
            # Process CONNECT packet
            client_id, clean_session, keep_alive = self.handle_connect(client_sock, packet)
            if not client_id:
                return

            with self.lock:
                # Register the client
                self.clients[client_sock] = client_id

            if not clean_session and client_id in self.client_sessions:
                # Restore previous subscriptions
                previous_subscriptions = self.client_sessions[client_id]['subscriptions']
                with self.lock:
                    for topic in previous_subscriptions:
                        if client_sock not in self.topic_subscribers:
                            self.topic_subscribers[client_sock] = set()
                        self.topic_subscribers[client_sock].add(topic)
            else:
                # Initialize session and subscriptions
                with self.lock:
                    self.client_sessions[client_id] = {'subscriptions': set()}
                    self.topic_subscribers[client_sock] = set()

            while True:
                # Receive other packets
                packet = self.receive_packet(client_sock)
                if not packet:
                    break
                last_packet_time = time.time()
                packet_type = packet[0] >> 4
                if packet_type == 3:
                    # Handle PUBLISH packet
                    self.handle_publish(client_sock, packet)
                elif packet_type == 8:
                    # Handle SUBSCRIBE packet
                    self.handle_subscribe(client_sock, packet)
                elif packet_type == 10:
                    # Handle UNSUBSCRIBE packet
                    self.handle_unsubscribe(client_sock, packet)
                elif packet_type == 12:
                    # Handle PINGREQ packet
                    self.handle_pingreq(client_sock)
                elif packet_type == 14:
                    # Handle DISCONNECT packet
                    print("Client requested disconnect.")
                    break
                else:
                    print(f"Unsupported packet type: {packet_type}")
                # Check for keep-alive timeout
                if keep_alive and (time.time() - last_packet_time) > (1.5 * keep_alive):
                    print("Client keep-alive timeout.")
                    break
        except Exception as e:
            print(f"Error handling client: {e}")
            traceback.print_exc()
        finally:
            # Clean up client resources
            client_sock.close()
            with self.lock:
                client_id = self.clients.pop(client_sock, None)
                if client_id:
                    if clean_session:
                        self.client_sessions.pop(client_id, None)
                    else:
                        self.client_sessions[client_id]['subscriptions'] = self.topic_subscribers.get(client_sock, set())
                    self.topic_subscribers.pop(client_sock, None)
            print(f"Client {client_id} disconnected.")

    def receive_packet(self, client_sock):
        # Receive a full MQTT packet from the client
        fixed_header = b''
        while True:
            # Read the fixed header
            data = client_sock.recv(1)
            if not data:
                return None
            fixed_header += data
            if len(fixed_header) >= 2:
                # Decode the remaining length
                remaining_length, bytes_read = self.decode_variable_length(fixed_header[1:])
                total_length = 1 + bytes_read + remaining_length
                break

        packet = fixed_header
        while len(packet) < total_length:
            # Read the remaining packet data
            data = client_sock.recv(total_length - len(packet))
            if not data:
                return None
            packet += data
        return packet

    def decode_variable_length(self, data):
        # Decode MQTT variable-length field
        multiplier = 1
        value = 0
        index = 0
        while True:
            if index >= len(data):
                raise Exception("Malformed Remaining Length")
            encoded_byte = data[index]
            value += (encoded_byte & 127) * multiplier
            if (encoded_byte & 128) == 0:
                index += 1
                break
            multiplier *= 128
            index += 1
            if multiplier > 128*128*128:
                raise Exception("Malformed Remaining Length")
        return value, index

    def handle_connect(self, client_sock, packet):
        # Handle CONNECT packet
        try:
            index = 2  # Start after fixed header

            # Protocol Name Length
            proto_name_len = struct.unpack('!H', packet[index:index+2])[0]
            index += 2

            # Protocol Name
            proto_name = packet[index:index+proto_name_len].decode()
            index += proto_name_len

            # Protocol Level
            proto_level = packet[index]
            index += 1

            # Connect Flags
            connect_flags = packet[index]
            index += 1

            # Keep Alive
            keep_alive = struct.unpack('!H', packet[index:index+2])[0]
            index += 2

            if proto_level == 5:
                # MQTT 5.0 - Decode and skip properties
                props_len, bytes_read = self.decode_variable_length(packet[index:])
                index += bytes_read + props_len

            # Client ID Length
            client_id_len = struct.unpack('!H', packet[index:index+2])[0]
            index += 2

            # Client ID
            client_id = packet[index:index+client_id_len].decode()
            index += client_id_len

            # Store protocol level in client session
            with self.lock:
                if client_id not in self.client_sessions:
                    self.client_sessions[client_id] = {'subscriptions': set()}
                self.client_sessions[client_id]['proto_level'] = proto_level

            # Prepare CONNACK response
            if proto_level == 5:
                # MQTT 5.0 CONNACK packet
                connack = bytearray()
                connack.append(0x20)  # Packet Type: CONNACK
                connack += self.encode_variable_length(3)  # Remaining Length
                connack.append(0x00)  # Connect Acknowledge Flags
                connack.append(0x00)  # Reason Code (Success)
                connack.append(0x00)  # Property Length (0)
            else:
                # MQTT 3.1.1 CONNACK packet
                connack = bytes([0x20, 0x02, 0x00, 0x00])

            client_sock.sendall(connack)
            print(f"Client '{client_id}' connected with protocol level {proto_level}.")

            # Extract Clean Session flag
            clean_session = (connect_flags & 0x02) >> 1

            return client_id, clean_session, keep_alive

        except Exception as e:
            print(f"Error handling CONNECT: {e}")
            traceback.print_exc()
            return None, None, None

    def handle_publish(self, client_sock, packet):
        # Handle PUBLISH packet
        try:
            index = 1
            # Decode Remaining Length
            remaining_length, bytes_read = self.decode_variable_length(packet[1:])
            index += bytes_read
            # Extract flags
            dup = (packet[0] & 0x08) >> 3
            qos = (packet[0] & 0x06) >> 1
            retain = packet[0] & 0x01
            # Topic Name Length
            topic_length = struct.unpack('!H', packet[index:index+2])[0]
            index += 2
            # Topic Name
            topic_name = packet[index:index+topic_length].decode('utf-8')
            index += topic_length
            if qos > 0:
                # Packet Identifier
                packet_id = struct.unpack('!H', packet[index:index+2])[0]
                index += 2
            # Skip properties (if any)
            if index < len(packet):
                properties_length, bytes_read = self.decode_variable_length(packet[index:])
                index += bytes_read + properties_length
            # Message Payload
            payload = packet[index:]
            message = payload.decode('utf-8')
            print(f"Received PUBLISH from '{self.clients[client_sock]}' on topic '{topic_name}' with QoS {qos}, Retain {retain}: {message}")

            # Handle retain message
            if retain:
                with self.lock:
                    if message:
                        # Store the retained message
                        self.retained_messages[topic_name] = packet
                        print(f"Retained message set for topic '{topic_name}'")
                    else:
                        # Remove the retained message if payload is empty
                        self.retained_messages.pop(topic_name, None)
                        print(f"Retained message cleared for topic '{topic_name}'")

            publish_packet = packet

            with self.lock:
                recipients = set()
                # Collect all subscribers who need to receive the message
                for subscriber_sock, topics in self.topic_subscribers.items():
                    for topic_filter in topics:
                        if self.topic_matches_filter(topic_name, topic_filter):
                            recipients.add(subscriber_sock)
                            break  # No need to check other filters for this subscriber

                # Send the message to each subscriber once
                for subscriber_sock in recipients:
                    subscriber_sock.sendall(publish_packet)
                    print(f"Message forwarded to '{self.clients[subscriber_sock]}'")

        except Exception as e:
            print(f"Error handling PUBLISH: {e}")
            traceback.print_exc()

    def handle_subscribe(self, client_sock, packet):
        # Handle SUBSCRIBE packet
        try:
            index = 1
            # Decode Remaining Length
            remaining_length, bytes_read = self.decode_variable_length(packet[1:])
            index += bytes_read
            # Packet Identifier
            packet_id = struct.unpack('!H', packet[index:index+2])[0]
            index += 2
            # Skip properties (if any)
            if index < len(packet):
                properties_length, bytes_read = self.decode_variable_length(packet[index:])
                index += bytes_read + properties_length
            requested_qos_list = []
            client_id = self.clients.get(client_sock, 'Unknown')
            while index < len(packet):
                if index + 2 > len(packet):
                    break
                # Topic Filter Length
                topic_length = struct.unpack('!H', packet[index:index+2])[0]
                index += 2
                if index + topic_length > len(packet):
                    break
                # Topic Filter
                topic_filter = packet[index:index+topic_length].decode('utf-8')
                index += topic_length
                if index >= len(packet):
                    subscription_options = 0  # Default QoS 0
                else:
                    # Subscription Options
                    subscription_options = packet[index]
                    index += 1
                requested_qos = subscription_options & 0x03
                requested_qos_list.append(requested_qos)
                with self.lock:
                    # Add subscription to session and subscriber list
                    self.client_sessions[client_id]['subscriptions'].add(topic_filter)
                    if client_sock not in self.topic_subscribers:
                        self.topic_subscribers[client_sock] = set()
                    self.topic_subscribers[client_sock].add(topic_filter)
                print(f"Client '{client_id}' subscribed to '{topic_filter}' with QoS {requested_qos}")

                # Send retained messages if any
                with self.lock:
                    for topic, retained_packet in self.retained_messages.items():
                        if self.topic_matches_filter(topic, topic_filter):
                            client_sock.sendall(retained_packet)
                            print(f"Sent retained message on '{topic}' to '{client_id}'")

            # Send SUBACK response
            suback = bytearray()
            suback.append(0x90)
            suback += self.encode_variable_length(2 + len(requested_qos_list))
            suback += struct.pack('!H', packet_id)
            for qos in requested_qos_list:
                suback.append(qos)
            client_sock.sendall(suback)
        except Exception as e:
            print(f"Error handling SUBSCRIBE: {e}")
            traceback.print_exc()

    def handle_unsubscribe(self, client_sock, packet):
        # Handle UNSUBSCRIBE packet
        try:
            index = 1
            # Decode Remaining Length
            remaining_length, bytes_read = self.decode_variable_length(packet[1:])
            index += bytes_read

            # Packet Identifier
            packet_id = struct.unpack('!H', packet[index:index+2])[0]
            index += 2

            # Get protocol level from client session
            client_id = self.clients.get(client_sock, 'Unknown')
            proto_level = self.client_sessions.get(client_id, {}).get('proto_level', 4)  # Default to 4 (MQTT 3.1.1)

            # Decode properties for MQTT 5.0
            if proto_level == 5:
                properties_length, bytes_read = self.decode_variable_length(packet[index:])
                index += bytes_read + properties_length  # Skip properties

            # Extract Topic Filters
            topic_filters = []
            while index < len(packet):
                # Topic Filter Length
                topic_length = struct.unpack('!H', packet[index:index+2])[0]
                index += 2
                # Topic Filter
                topic_filter = packet[index:index+topic_length].decode('utf-8')
                index += topic_length
                topic_filters.append(topic_filter)

            with self.lock:
                # Remove subscriptions
                for topic_filter in topic_filters:
                    self.client_sessions[client_id]['subscriptions'].discard(topic_filter)
                    if client_sock in self.topic_subscribers:
                        self.topic_subscribers[client_sock].discard(topic_filter)
                    print(f"Client '{client_id}' unsubscribed from '{topic_filter}'")
                if not self.topic_subscribers.get(client_sock):
                    self.topic_subscribers.pop(client_sock, None)

            # Construct UNSUBACK response
            if proto_level == 5:
                # MQTT 5.0 UNSUBACK packet
                unsuback = bytearray()
                unsuback.append(0xB0)  # Packet Type: UNSUBACK

                # Property Length (no properties)
                property_length = 0
                property_length_encoded = self.encode_variable_length(property_length)

                # Reason Codes (one per topic filter)
                reason_codes = bytearray()
                for _ in topic_filters:
                    reason_codes.append(0x00)  # Reason Code 0x00 (Success)

                # Calculate Remaining Length
                remaining_length = (
                    2  # Packet Identifier
                    + len(property_length_encoded)  # Property Length field size
                    + len(reason_codes)  # Reason Codes size
                )

                # Encode Remaining Length
                remaining_length_encoded = self.encode_variable_length(remaining_length)
                unsuback.extend(remaining_length_encoded)

                # Packet Identifier
                unsuback.extend(struct.pack('!H', packet_id))

                # Property Length
                unsuback.extend(property_length_encoded)

                # Reason Codes
                unsuback.extend(reason_codes)
            else:
                # MQTT 3.1.1 UNSUBACK packet
                unsuback = bytearray()
                unsuback.append(0xB0)  # Packet Type: UNSUBACK
                unsuback.append(0x02)  # Remaining Length
                unsuback.extend(struct.pack('!H', packet_id))

            # Send the UNSUBACK packet
            client_sock.sendall(unsuback)
            print(f"UNSUBACK sent to client '{client_id}'")
        except Exception as e:
            print(f"Error handling UNSUBSCRIBE: {e}")
            traceback.print_exc()

    def handle_pingreq(self, client_sock):
        # Handle PINGREQ packet
        try:
            # Respond with PINGRESP
            pingresp = bytearray()
            pingresp.append(0xD0)
            pingresp += self.encode_variable_length(0)
            client_sock.sendall(pingresp)
            print("PINGREQ received, sent PINGRESP.")
        except Exception as e:
            print(f"Error handling PINGREQ: {e}")
            traceback.print_exc()

    def encode_variable_length(self, value):
        # Encode MQTT variable-length integer
        encoded = bytearray()
        if value == 0:
            encoded.append(0)
        else:
            while value > 0:
                digit = value % 128
                value = value // 128
                # If there are more digits, set the top bit of this digit
                if value > 0:
                    digit |= 0x80
                encoded.append(digit)
        return encoded

    def get_variable_length_bytes(self, value):
        # Calculate the number of bytes needed for a Variable Byte Integer
        count = 0
        while True:
            count += 1
            value = value // 128
            if value == 0:
                break
        return count

    def topic_matches_filter(self, topic, topic_filter):
        # Match topic against topic filter with wildcards
        topic_levels = topic.split('/')
        filter_levels = topic_filter.split('/')
        t_len = len(topic_levels)
        f_len = len(filter_levels)
        t_index = 0
        f_index = 0
        while f_index < f_len:
            if filter_levels[f_index] == '#':
                # Multi-level wildcard matches any remaining levels
                return True
            elif filter_levels[f_index] == '+':
                # Single-level wildcard matches one topic level
                if t_index >= t_len:
                    return False
            else:
                # Exact match is required
                if t_index >= t_len or filter_levels[f_index] != topic_levels[t_index]:
                    return False
            f_index += 1
            t_index += 1
        return t_index == t_len

if __name__ == '__main__':
    broker = MQTTBroker()
    try:
        broker.start()
    except KeyboardInterrupt:
        print("Broker shut down.")
        sys.exit(0)