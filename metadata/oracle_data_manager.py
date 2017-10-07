# import cx_Oracle

class OracleDataManager:

    def fetch_no_of_rows_from_oracle(self, table_name):

        # conn = cx_Oracle.connect("user/password@host:port/service")
        # cur = conn.cursor()
        # value_ = cur.execute("SELECT COUNT(*) FROM %s" % (table_name))

        import MySQLdb

        db = MySQLdb.connect(host="db4free.net", user="hector", passwd="hector", db="hectordb", port=3307)

        cursor = db.cursor()

        # execute SQL query using execute() method.
        data = cursor.execute("SELECT COUNT(*) FROM %s" % (table_name))

        print "Fetching data from oracle ... "
        print data
        db.close()