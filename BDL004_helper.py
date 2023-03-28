"""
FYP : 22013

Module:
    Data Loader - Helper

Description:
    This python script contains the helper functions that help with the
    interactions with the Server Skeleton SUT.
"""

# Imports from built-in modules
import json
import socket

def send_json(socket_to_server, dictionary_to_send):
    message = json.dumps(dictionary_to_send) + '|'
    socket_to_server.sendall(message.encode('ascii'))

def recv_json(socket_to_server):
    message = ""
    while True:
        curr_char = socket_to_server.recv(1)
        if curr_char.decode('ascii') == '|':
            break
        else:
            message += curr_char.decode('ascii')

    return json.loads(message)
