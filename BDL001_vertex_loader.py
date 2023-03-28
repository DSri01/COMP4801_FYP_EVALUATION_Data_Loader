"""
FYP : 22013

Module:
    Data Loader - Vertex Loader

Description:
    This python script contains the definition of the Vertex_Loader class and
    the FreshnessScore_Vertex_Loader class.

    The Vertex_Loader class extends the Multithreaded_Object_loader class to
    load the data of different vertex types from the base data files to the
    Server Skeleton SUT. The FreshnessScore_Vertex_Loader class loads the
    freshness score vertices to the SUT (no input files needed).
"""

# Imports from built-in modules
import socket

# Imports from Data Loader module
from BDL000_multithreaded_object_loader import Multithreaded_Object_loader
from BDL004_helper import send_json, recv_json

class Vertex_Loader(Multithreaded_Object_loader):

    def __init__(self, number_of_threads=2,\
                 file_name="test.txt",\
                 lines_per_thread=1000,\
                 current_start_line_number = 1,\
                 IP_ADDRESS = "127.0.0.1",\
                 PORT_NUMBER = 12560,\
                 OPERATION_ID = 0,\
                 OBJECT_NAME="vertex",\
                 ATTRIBUTE_TYPE="NAME"):

        super().__init__(number_of_threads,\
                         file_name,\
                         lines_per_thread,\
                         current_start_line_number,\
                         IP_ADDRESS,\
                         PORT_NUMBER,\
                         OPERATION_ID,\
                         OBJECT_NAME)

        self.ATTRIBUTE_TYPE = ATTRIBUTE_TYPE

    def parse_line(self, line):
        dict = {}
        is_line_empty = True
        if '|' in line:
            is_line_empty = False
            data_point = line.strip('\n').split('|')

            dict["ID"] = int(data_point[0])

            dict["OPERATION_ID"] = self.OPERATION_ID

            if self.ATTRIBUTE_TYPE == "NAME":
                dict["NAME"] = data_point[1]
            else:
                dict["AMOUNT"] = int(data_point[1])
        return dict, is_line_empty


class FreshnessScore_Vertex_Loader:
    def __init__(self, number_of_transactional_threads=100,\
                 freshness_score_start_ID=100,\
                 IP_ADDRESS = "127.0.0.1",\
                 PORT_NUMBER = 12560,\
                 OPERATION_ID = 0,\
                 ):
        self.number_of_transactional_threads = number_of_transactional_threads
        self.freshness_score_start_ID = freshness_score_start_ID
        self.IP_ADDRESS = IP_ADDRESS
        self.PORT_NUMBER = PORT_NUMBER
        self.OPERATION_ID = OPERATION_ID

    def execute(self):
        for i in range(0, self.number_of_transactional_threads):
            message_dict = {}
            message_dict["OPERATION_ID"] = self.OPERATION_ID
            message_dict["ID"] = self.freshness_score_start_ID + i
            message_dict["TRANSACTION_ID"] = 0
            socket_to_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket_to_server.connect((self.IP_ADDRESS, self.PORT_NUMBER))
            send_json(socket_to_server, message_dict)
            response_dict = recv_json(socket_to_server)

            if (response_dict["SUCCESS"] == False):
                print("ERROR IN LOADING FRESHNESS SCORE VERTEX")

            socket_to_server.close()
        print("FRESHNESS SCORE LOAD COMPLETE")
