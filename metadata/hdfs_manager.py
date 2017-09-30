import subprocess

from metadata.metadata_util import find_all_xml_files_in_hdfs


class HDFSManager:

    def fetchXMLFileFromHDFS(self):

        cat = subprocess.Popen("ls", stdout=subprocess.PIPE)
        for line in cat.stdout:
            print line
        return


    def fetch_the_recent_xml_file_from_hdfs(self):

        list_of_xml_files =self.fetch_all_xml_files_from_hdfs("")
        print list_of_xml_files

        return

    def fetch_all_xml_files_from_hdfs(self, hdfs_dir_path):
        return find_all_xml_files_in_hdfs(hdfs_dir_path)

