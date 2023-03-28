"""
FYP : 22013

Module:
    Data Loader - General Object Loader

Description:
    This python script contains the definition of the Multithreaded_Object_loader
    class.

    The Multithreaded_Object_loader class is the base class for all object loader
    classes in the module like the 'Vertex_Loader' class. These classes extend
    the Multithreaded_Object_loader to provide their functionality.

"""

# Imports from built-in modules
import json
import socket
import threading

# Imports from Data Loader module
from BDL004_helper import send_json, recv_json

class Multithreaded_Object_loader:

    """
    Description:
        Contains functionality to load any 'object' to the Server Skeleton SUT
        from the base data files generated by the Base Data Generator in a
        multithreaded fashion. The subclasses only need to define the following
        method:

            - parse_line(line): specifies how the inputted file line 'line'
                                should be parsed to generate a 'query' to the
                                Server Skeleton SUT to add the object to the
                                graph.
    """

    def __init__(self, number_of_threads=2,\
                 file_name="test.txt",\
                 lines_per_thread=1000,\
                 current_start_line_number = 1,\
                 IP_ADDRESS = "127.0.0.1",\
                 PORT_NUMBER = 12560,\
                 OPERATION_ID = 0,\
                 OBJECT_NAME="vertex"):

        self.number_of_threads = number_of_threads
        self.lines_per_thread = lines_per_thread
        self.file_pointer = open(file_name, 'r')
        count = 1
        for line in self.file_pointer:
            if count >= current_start_line_number:
                break
            else:
                count += 1
        self.next_line_batch_lock = threading.Lock()
        self.terminated_thread_count = 0
        self.termination_lock = threading.Lock()

        self.IP_ADDRESS = IP_ADDRESS
        self.PORT_NUMBER = PORT_NUMBER
        self.OPERATION_ID = OPERATION_ID
        self.OBJECT_NAME = OBJECT_NAME

    def parse_line(self, line):
        dict = {}
        is_line_empty = True
        if '|' in line:
            is_line_empty = False
            #further parse in child classes
        return dict, is_line_empty

    def fetch_next_line_batch(self):
        self.next_line_batch_lock.acquire()
        batch = []
        count = 0
        for line in self.file_pointer:
            batch.append(line)
            count += 1
            if count > self.lines_per_thread:
                break
        self.next_line_batch_lock.release()
        return batch

    def thread_done(self):
        self.termination_lock.acquire()
        self.terminated_thread_count += 1
        self.termination_lock.release()

    def thread_job(self):
        while True:
            current_batch = self.fetch_next_line_batch()
            if len(current_batch) <= 0:
                break

            for line in current_batch:
                parsed_dict, is_line_empty = self.parse_line(line)
                if is_line_empty:
                    continue
                else:
                    socket_to_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    socket_to_server.connect((self.IP_ADDRESS, self.PORT_NUMBER))
                    send_json(socket_to_server,parsed_dict)
                    response_dict = recv_json(socket_to_server)

                    if (response_dict["SUCCESS"] == False):
                        print("ERROR IN LOADING", self.OBJECT_NAME, "Line: ", line)

                    socket_to_server.close()

        self.thread_done()


    def execute(self):
        thread_list = []
        for i in range(0, self.number_of_threads):
            temp_thread_object = threading.Thread(target=self.thread_job)
            temp_thread_object.start()
            thread_list.append(temp_thread_object)

        for thread in thread_list:
            thread.join()
        self.file_pointer.close()
        print(self.OBJECT_NAME, "LOAD COMPLETE")
