
import time
import datetime
import subprocess as sp
import os



TEMP_XML_FILE_LOCATION = "/tmp/metrolinx.xml"

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
def find_all_xml_files_in_hdfs(hdfs_dir_path):

    # To fetch the recent xml use the command - hdfs dfs -ls -C -t /metro*
    list_of_xml_files = []
    all_xml_files = sp.Popen(["hdfs", "dfs", "-ls", "-C", "/*.xml"], stdout=sp.PIPE).communicate()[0]

    for each_file in all_xml_files.splitlines():
        list_of_xml_files.append(each_file.replace("/",""))

    return list_of_xml_files



#
#
#
def get_process_id_of_application(application_name):
        #process_id = sp.Popen(["ps", "-aux", "|",  "grep", "-i", "hdfs", "|",  "awk" , "{'print $2'}"], stdout=sp.PIPE).communicate()[0]
        list_of_processes = sp.Popen(["ps", "-aux"], stdout=sp.PIPE).communicate()[0]
        for each_line in list_of_processes.splitlines():
            if (each_line.__contains__("hdfs")):
                print each_line.split(" ")[1]
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



def get_current_process_id():
    return os.getpid()
#
# Returns the parent process name
#
def get_parent_process_id():
    return os.getppid()

class MetadataValue:

    def __init__(self, **args):
        self.__dict__= args


class MetadataCleanerService:

    def cleanFiles(self):

        import os
        os.remove(TEMP_XML_FILE_LOCATION)
        return


class MetadataJobDetailComputingManager:

    # python metadata.egg
    def fetchJOBDetail(self):
        metadatavalue = MetadataValue()

        get_process_id_of_application("")

        metadatavalue.op_id = "OP_ID"       # get from linux hive or oozie  1 or 2
        metadatavalue.op_name = "OPNAME"    # Fetch it from xml
        metadatavalue.process_id= get_current_process_id()
        metadatavalue.op_start_time_stamp = get_current_time()
        metadatavalue.op_end_time_stamp = get_current_time()
        metadatavalue.op_status = "RUNNING"  # from command line
        metadatavalue.op_owner = get_current_linux_user_name()
        metadatavalue.record_count = 100   # Need to get it from log file
        metadatavalue.op_parent_process_name = get_parent_process_id() # from running or started or completed

        return metadatavalue