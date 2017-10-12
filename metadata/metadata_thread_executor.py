import thread

class MetadataThread:

    def __init__(self, _query):
        self.query = _query

    def executeInThread(self):
        import commands as co
        co.getstatusoutput(self.query)

    def executeJobInThread(self):
        thread.start_new_thread(self.executeInThread(),  ("T"))