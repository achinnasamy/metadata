import subprocess


class HDFSManager:

    def fetchXMLFileFromHDFS(self):

        cat = subprocess.Popen("ls", stdout=subprocess.PIPE)
        for line in cat.stdout:
            print line


        return