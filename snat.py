#! /usr/bin/env python

import sys, base64
from suds.client import Client
from rpclib.model.binary import ByteArray

soapclient = Client('http://localhost:7791/?wsdl')
serv = soapclient.service

if(len(sys.argv) > 1):
    
    if(sys.argv[1] == 'e'): #execute
 
        result = serv.execute_algorithm(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5:]) #algorithm_id, data_set_id, num_nodes, command_line_args
        print base64.b64decode(result)
        
    elif(sys.argv[1][0] == 'l'): #list

        if(sys.argv[1][1] == 'a'): #algorithms
            print serv.get_algorithms()
        
        elif(sys.argv[1][1] == 'd'): #datasets
            print serv.get_data_sets()
        
        
    elif(sys.argv[1][0] == 'u'): #upload
        
        f = open(sys.argv[3])#read in the file
        fileContents = base64.b64encode(f.read()) #bytearray(f.read()
        
        if(sys.argv[1][1] == 'a'): #algorithm
            # args = ' '.join(sys.argv[4:])
            print serv.upload_algorithm(sys.argv[2], sys.argv[4], fileContents)
        
        if(sys.argv[1][1] == 'd'): #dataset
            print serv.upload_data_set(sys.argv[2], fileContents)
    
    
    elif(sys.argv[1] == 's'): #status
        print serv.show_status("50070/dfshealth.jsp")
        
        
else:
    print """Usage: \n"""+sys.argv[0] + """ 
        s                                                       Status
        e    <algorithm id> <dataset id> <#nodes> <args>        Execute
        la                                                      List algorithms
        ld                                                      List datasets
        ua   <name> <filename> <job class>                      Upload algorithm
        ud   <name> <filename>                                  Upload dataset
"""
