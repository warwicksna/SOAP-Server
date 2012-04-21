#!/usr/bin/env python
# encoding: utf8
#
# Copyright © Burak Arslan <burak at arskom dot com dot tr>,
#             Arskom Ltd. http://www.arskom.com.tr
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#    3. Neither the name of the owner nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
# OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import logging
import subprocess
import re
import base64

from time import time
from threading import Thread
from rpclib.decorator import srpc
from rpclib.service import ServiceBase
from rpclib.model.complex import Iterable
from rpclib.model.primitive import Integer
from rpclib.model.primitive import String
from rpclib.model.binary import ByteArray
from rpclib.util.simple import wsgi_soap_application

try:
    from wsgiref.simple_server import make_server
except ImportError:
    print("Error: example server code requires Python >= 2.5")
    raise

base_dfs_dir = "/user/hduser/"
hadoop_dir = "/usr/local/hadoop/"
pool = {}
pool_id = 0
results = {}

def list_algorithms():
    myfile = open(hadoop_dir+"algorithms/algorithms.txt","r")
    algos = [line.split() for line in myfile.read().split("\n") if line != ""]
    return algos

def list_datasets():
    str = subprocess.check_output([hadoop_dir+"bin/hadoop","dfs","-ls",base_dfs_dir+"snat_datasets/"])
    datasets = [re.search(base_dfs_dir+'snat_datasets/([A-z0-9_-]+)',line).group(1) for line in str.split("\n")[1:-1]]
    return datasets

def run_script(script_name, t_id):
    cmd = ["python",hadoop_dir+"algorithms/"+script_name]
    ret = subprocess.check_output(cmd)
    output = re.search("OUTPUT:([A-z0-9_-]+)",ret).group(1)
    results[t_id] = base_dfs_dir+"snat_output/"+output

def run_jar(data_set_name, algorithm_name, jarfile, classname, t_id):
    input = base_dfs_dir+"snat_datasets/"+data_set_name
    output = base_dfs_dir+"snat_output/"+algorithm_name+"-"+data_set_name+"-"+str(int(time()))
    cmd = [hadoop_dir+"bin/hadoop","jar",hadoop_dir+"algorithms/"+jarfile,classname,input,output]
    subprocess.call(cmd)
    results[t_id] = output

def add_to_pool(t):
    global pool
    global pool_id
    pool[pool_id] = t
    pool_id = pool_id + 1

class SnatService(ServiceBase):

    @srpc(String, String, _returns=String)
    def upload_data_set(data_set_name, data_set):
        tmp_file_name = "/tmp/snat_upload."+data_set_name+".txt"

        # check if dataset of that name already exists
        dataset = [ds for ds in list_datasets() if ds == data_set_name]
        if len(dataset) != 0:
            return "fail! Dataset of that name already exists"

        # delete existing temp file, if it exists
        cmd = ["rm",tmp_file_name]
        subprocess.call(cmd)        
        myfile = open(tmp_file_name,"w")
        myfile.write(base64.b64decode(data_set))
        myfile.close()
        
        cmd = [hadoop_dir+"bin/hadoop",
               "dfs",
               "-copyFromLocal",
               tmp_file_name,
               base_dfs_dir+"snat_datasets/"+data_set_name]
        subprocess.call(cmd)

        return "success"

    @srpc(String, String, String, _returns=String)
    def upload_algorithm(algorithm_name, class_name, jar_file):
        algo = [algo for algo in list_algorithms() if algo[0] == algorithm_name]
        if len(algo) != 0:
            return "Algorithm with that name exists!"

        jar_file_name = hadoop_dir + "algorithms/" + algorithm_name + ".jar"
        
        # delete existing jar file, if it exists
        cmd = ["rm",jar_file_name]
        subprocess.call(cmd)

        myfile = open(jar_file_name,"w")
        myfile.write(base64.b64decode(jar_file))
        myfile.close()

        myfile = open(hadoop_dir+"algorithms/algorithms.txt","a+")
        myfile.write("\n"+algorithm_name+" "+algorithm_name+".jar "+class_name)
        myfile.close()
        return "Success!"

    @srpc(_returns=Iterable(String))
    def get_algorithms():
        return [algo[0] for algo in list_algorithms()]

    @srpc(_returns=Iterable(String))
    def get_data_sets():
        return list_datasets()

    @srpc(String, String, Integer, String, _returns=Integer)
    def execute_algorithm(algorithm_name, data_set_name, num_nodes, command_line_args):

        algo = [algo for algo in list_algorithms() if algo[0] == algorithm_name][0]
        dataset = [ds for ds in list_datasets() if ds == data_set_name]

        if len(algo) == 0:
            error = "No Algorithm with name: "+algorithm_name
        elif len(dataset) == 0:
            error = "No data set with name: "+data_set_name
        else:
            t_id = pool_id
            if algo[1] == "jar":
                t = Thread(target=run_jar, args=(data_set_name, algorithm_name, algo[2], algo[3], t_id))
            elif algo[1] == "py":
                t = Thread(target=run_script, args=(algo[2],t_id))

            t.start()
            add_to_pool(t)
            return t_id

        logging.error(error)
        return 0
                   
    @srpc(Integer, _returns=String)
    def get_results(t_id):
        if pool[t_id].isAlive():
            return("0")
        else:
            page = results[t_id]
            cmd = [hadoop_dir+"bin/hadoop","dfs","-cat",page+"/part-r-00000"]
            ret = subprocess.check_output(cmd)
            return base64.b64encode(ret)


    @srpc(String, _returns=String)
    def show_status(required_data):
        # TODO: pass proper data to required_data 
        cmd = ["curl","localhost:"+required_data]
        return subprocess.check_output(cmd)
    

if __name__=='__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('rpclib.protocol.xml').setLevel(logging.DEBUG)

    wsgi_app = wsgi_soap_application([SnatService], 'warwick.snat.soap')
    server = make_server('127.0.0.1', 7792, wsgi_app)
    server.serve_forever()
