"""
FYP : 22013

Module:
    Data Loader (also called Bulk Data Loader)

Description:
    This python script executes the necessary code in the Data Loader module to
    load base data to the System Under Test (SUT) before any experimentation
    begins.
"""

# Imports from built-in modules
import json
import multiprocessing as mp
import sys

# Imports from Data Loader module
from BDL001_vertex_loader import Vertex_Loader
from BDL001_vertex_loader import FreshnessScore_Vertex_Loader
from BDL002_edge_loader import Friend_OR_Mirror_Edge_Loader
from BDL002_edge_loader import HasBook_Edge_Loader

# Loads Investor vertices to the System Under Test (SUT)
def load_investor_vertices(config):
    investor_loader = Vertex_Loader(number_of_threads=1,\
                 file_name=config["investor_name_file_name"],\
                 lines_per_thread=1000,\
                 current_start_line_number = 1,\
                 IP_ADDRESS = config["IP_ADDRESS"],\
                 PORT_NUMBER = config["PORT_NUMBER"],\
                 OPERATION_ID = 1,\
                 OBJECT_NAME="INVESTOR",\
                 ATTRIBUTE_TYPE="NAME")

    investor_loader.execute()

# Loads TradeBook vertices to the System Under Test (SUT)
def load_tradebook_vertices(config):
    tradebook_loader = Vertex_Loader(number_of_threads=10,\
                 file_name=config["tradebook_investment_amount_file_name"],\
                 lines_per_thread=1000,\
                 current_start_line_number = 1,\
                 IP_ADDRESS = config["IP_ADDRESS"],\
                 PORT_NUMBER = config["PORT_NUMBER"],\
                 OPERATION_ID = 2,\
                 OBJECT_NAME="TRADEBOOK",\
                 ATTRIBUTE_TYPE="AMOUNT")

    tradebook_loader.execute()

# Loads Company vertices to the System Under Test (SUT)
def load_company_vertices(config):
    company_loader = Vertex_Loader(number_of_threads=10,\
                 file_name=config["company_name_file_name"],\
                 lines_per_thread=1000,\
                 current_start_line_number = 1,\
                 IP_ADDRESS = config["IP_ADDRESS"],\
                 PORT_NUMBER = config["PORT_NUMBER"],\
                 OPERATION_ID = 3,\
                 OBJECT_NAME="COMPANY",\
                 ATTRIBUTE_TYPE="NAME")

    company_loader.execute()

# Loads Freshness Score vertices to the System Under Test (SUT)
def load_freshness_score_vertices(config):
    fs_vertex_loader = FreshnessScore_Vertex_Loader(number_of_transactional_threads=config["number_of_transactional_threads"],\
                 freshness_score_start_ID=config["number_of_investors"]*2+config["number_of_companies"],\
                 IP_ADDRESS = config["IP_ADDRESS"],\
                 PORT_NUMBER = config["PORT_NUMBER"],\
                 OPERATION_ID = 4)

    fs_vertex_loader.execute()

# Loads Friend edges to the System Under Test (SUT)
def load_friend_edges(config):
    friend_edge_loader = Friend_OR_Mirror_Edge_Loader(number_of_threads=2,\
                 file_name=config["friend_edges_file_name"],\
                 lines_per_thread=1000,\
                 current_start_line_number = 2,\
                 IP_ADDRESS = config["IP_ADDRESS"],\
                 PORT_NUMBER = config["PORT_NUMBER"],\
                 OPERATION_ID = 5,\
                 OBJECT_NAME="FRIEND_EDGE")

    friend_edge_loader.execute()

# Loads Mirror edges to the System Under Test (SUT)
def load_mirror_edges(config):
    mirror_edge_loader = Friend_OR_Mirror_Edge_Loader(number_of_threads=2,\
                 file_name=config["mirror_edges_file_name"],\
                 lines_per_thread=1000,\
                 current_start_line_number = 2,\
                 IP_ADDRESS = config["IP_ADDRESS"],\
                 PORT_NUMBER = config["PORT_NUMBER"],\
                 OPERATION_ID = 6,\
                 OBJECT_NAME="MIRROR_EDGE")

    mirror_edge_loader.execute()

# Loads HasBook edges to the System Under Test (SUT)
def load_hasbook_edges(config):
    hasbook_edge_loader = HasBook_Edge_Loader(number_of_investors=config["number_of_investors"],\
                 IP_ADDRESS = config["IP_ADDRESS"],\
                 PORT_NUMBER = config["PORT_NUMBER"],\
                 OPERATION_ID = 7)

    hasbook_edge_loader.execute()

# Starts the
def start_bulk_data_loader(config):
    #create processes for vertices, load vertices first, then edges
    investor_vertex_process = mp.Process(target=load_investor_vertices,args=(config,))

    investor_vertex_process.start()
    investor_vertex_process.join()

    tradebook_vertex_process = mp.Process(target=load_tradebook_vertices,args=(config,))
    company_vertex_process = mp.Process(target=load_company_vertices,args=(config,))
    fs_vertex_process = mp.Process(target=load_freshness_score_vertices,args=(config,))

    tradebook_vertex_process.start()
    company_vertex_process.start()
    fs_vertex_process.start()

    #defining edge loading processes here, but they are started after vertices are loaded
    friend_edge_process = mp.Process(target=load_friend_edges,args=(config,))
    mirror_edge_process = mp.Process(target=load_mirror_edges,args=(config,))
    hasbook_edge_process = mp.Process(target=load_hasbook_edges,args=(config,))
    #start edge loading after vertices

    tradebook_vertex_process.join()
    company_vertex_process.join()
    fs_vertex_process.join()

    friend_edge_process.start()
    mirror_edge_process.start()
    hasbook_edge_process.start()

    friend_edge_process.join()
    mirror_edge_process.join()
    hasbook_edge_process.join()

    print("BASE DATA LOADED TO SERVER")

if __name__ == "__main__":
    if len(sys.argv) == 2:
        with open(sys.argv[1], 'r') as f_pointer:
            json_config = f_pointer.read()
            f_pointer.close()
            config = json.loads(json_config)
            start_bulk_data_loader(config)
    else:
        print("Usage:", sys.argv[0], "<CONFIG FILENAME>")
