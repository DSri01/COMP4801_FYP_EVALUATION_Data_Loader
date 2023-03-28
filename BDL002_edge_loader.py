"""
FYP : 22013

Module:
    Data Loader - Edge Loader

Description:
    This python script contains the definition of the
    Friend_OR_Mirror_Edge_Loader class and the HasBook_Edge_Loader class.

    The Friend_OR_Mirror_Edge_Loader class extends the Multithreaded_Object_loader
    class to load the data of different friend and mirror edges from the base data
    files to the Server Skeleton SUT. The HasBook_Edge_Loader class loads the
    HasBook edges to the SUT (no input files needed).
"""

# Imports from built-in modules
import socket

# Imports from Data Loader module
from BDL000_multithreaded_object_loader import Multithreaded_Object_loader
from BDL004_helper import send_json, recv_json

class Friend_OR_Mirror_Edge_Loader(Multithreaded_Object_loader):

    def __init__(self, number_of_threads=2,\
                 file_name="test.txt",\
                 lines_per_thread=1000,\
                 current_start_line_number = 2,\
                 IP_ADDRESS = "127.0.0.1",\
                 PORT_NUMBER = 12560,\
                 OPERATION_ID = 0,\
                 OBJECT_NAME="vertex"):

        super().__init__(number_of_threads,\
                         file_name,\
                         lines_per_thread,\
                         current_start_line_number,\
                         IP_ADDRESS,\
                         PORT_NUMBER,\
                         OPERATION_ID,\
                         OBJECT_NAME)

    def parse_line(self, line):
        dict = {}
        is_line_empty = True
        if '|' in line:
            is_line_empty = False
            data_point = line.strip('\n').split('|')
            dict["OPERATION_ID"] = self.OPERATION_ID
            dict["SOURCE_ID"] = int(data_point[0])
            dict["DESTINATION_ID"] = int(data_point[1])

        return dict, is_line_empty

class HasBook_Edge_Loader:
    def __init__(self, number_of_investors=100,\
                 IP_ADDRESS = "127.0.0.1",\
                 PORT_NUMBER = 12560,\
                 OPERATION_ID = 0,\
                 ):
        self.number_of_investors = number_of_investors
        self.IP_ADDRESS = IP_ADDRESS
        self.PORT_NUMBER = PORT_NUMBER
        self.OPERATION_ID = OPERATION_ID

    def execute(self):
        for i in range(0, self.number_of_investors):
            message_dict = {}
            message_dict["OPERATION_ID"] = self.OPERATION_ID
            message_dict["SOURCE_ID"] = i
            message_dict["TRADEBOOK_ID"] = i + self.number_of_investors
            socket_to_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket_to_server.connect((self.IP_ADDRESS, self.PORT_NUMBER))
            send_json(socket_to_server, message_dict)
            response_dict = recv_json(socket_to_server)

            if (response_dict["SUCCESS"] == False):
                print("ERROR IN LOADING HASBOOK EDGE")

            socket_to_server.close()
        print("HASBOOK EDGE LOAD COMPLETE")
