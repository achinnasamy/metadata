

#from subprocess import check_output

import time
import datetime
import subprocess as sp


#
#  Gets the process id of java
#
# print get_pid('java')
def get_pid(name):
    #return check_output(["pidof",name])
    return

def get_current_time():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M:%S')


#
# Find all xml files and put it in a list and send it back
#
def find_all_xml_files_in_hdfs():
    list_of_xml_files = []
    result = sp.Popen(["hdfs", "dfs", "-ls", "-C", "/*.xml"], stdout=sp.PIPE).communicate()[0]
    return


def execute_hdfs(filename):

    #result = sp.check_output('hdfs dfs -cat /metrolinux_metadatav2.xml')

    result = sp.Popen(["hdfs", "dfs", "-cat", "/metrolinx.xml"], stdout=sp.PIPE).communicate()[0]

    return result

def execute_query(query):

    sp.Popen(query, shell=True, stdout=sp.PIPE)
    #sp.call(query, shell=True)
    return

#
# Returns the current user name
#
def get_current_linux_user_name():
    import getpass
    return getpass.getuser()

#
# Returns the parent process name
#
def get_parent_process_id():
    import os
    return os.getppid()

class MetadataValue:

    def __init__(self, **args):
        self.__dict__= args


class MetadataCleanerService:

    def cleanFiles(self):

        import os
        os.remove("/tmp/metadata.xml")
        return


class MetadataJobDetailComputingManager:

    def fetchJOBDetail(self):
        metadatavalue = MetadataValue()

        metadatavalue.op_name = "OPNAME"
        metadatavalue.process_id= "1000"
        metadatavalue.op_start_time_stamp = get_current_time()
        metadatavalue.op_end_time_stamp = get_current_time()
        metadatavalue.op_eta = "10"
        metadatavalue.op_status = "RUNNING"
        metadatavalue.op_owner = get_current_linux_user_name()
        metadatavalue.record_count = 100
        metadatavalue.op_parent_process_name = "PARENT"

        return metadatavalue