import os
import psycopg2
from timeit import default_timer as timer
import numpy as np
class PostGreSQLDatabase:
    # Initialize MariaDBDatabase Object and creates
    # Cursor and Table to interact with the Database
    def __init__(self, host,user, password, database):
        try:
            self.con =psycopg2.connect(host=host, user=user, password=password, database=database)
        except:
            print("Datenbank konnte nicht geladen werden")
            SystemExit
        self.myCursor = self.con.cursor()
        self.createTable()

    def createTable(self):
        try:
            self.myCursor.execute("""CREATE TABLE COMETS(
            ParticleState_x FLOAT,
            ParticleState_y FLOAT,
            ParticleState_z FLOAT,
            ParticleState_Vx FLOAT,
            ParticleState_Vy FLOAT,
            ParticleState_Vz FLOAT,
            ETinSeconds FLOAT)
            """)
        except:
            pass

    #Create Insert String for the SQL statement
    def sqlStringMaker(self,byteArray):
        sqlString2 = []
        for i in range(0, len(byteArray), 7):
            sqlString2.append("(")
            sqlString2.append(str(byteArray[i]))
            sqlString2.append(",")
            sqlString2.append(str(byteArray[i + 1]))
            sqlString2.append(",")
            sqlString2.append(str(byteArray[i + 2]))
            sqlString2.append(",")
            sqlString2.append(str(byteArray[i + 3]))
            sqlString2.append(",")
            sqlString2.append(str(byteArray[i + 4]))
            sqlString2.append(",")
            sqlString2.append(str(byteArray[i + 5]))
            sqlString2.append(",")
            sqlString2.append(str(byteArray[i + 6]))
            sqlString2.append(")")
            sqlString2.append(",")
        return "".join(sqlString2)

    def insertComet(self, path):
        totalTime = 0
        cometNumber = 1
        fileList = []
        with os.scandir(path) as it:
            for entry in it:
                if entry.name.endswith(".ctwu") and entry.is_file():
                    fileList.append(entry.path)

        fileList.sort()

        for path in fileList:
            start = timer()
            with open(path, 'rb') as file:
                floatValues = np.array(np.fromfile(file, dtype=np.float32))
            s = self.sqlStringMaker(floatValues)
            end = timer()
            print(cometNumber)
            cometNumber = cometNumber + 1
            self.myCursor.execute("INSERT INTO COMETS VALUES  " + s[0:-1])
            end = timer()
            totalTime += (end - start)
            print(end - start)
        print(totalTime)

        # Searches particle given a timespan time1 - time 2
        def searchParticle(self, time1, time2):
            start = timer()
            self.myCursor.execute(
                "SELECT * FROM particleComets WHERE ETinSeconds BETWEEN ? AND ? ORDER BY ETinSeconds ASC",
                (time1, time2))
            result = self.myCursor.fetchall()
            end = timer()
            print(end - start)
            return result

        # Defines a testcase how fast Database retrieves Data when queries overlap
        # Timespan is one year
        def oneYearTestcase(self):
            self.searchParticle(1000000000, 1031536000)
            self.searchParticle(1000172800, 1031708800)

        # Same semantics as oneYearTestcase Method but timespan are two years
        def twoYearTestcase(self):
            self.searchParticle(1000000000, 1063072000)
            self.searchParticle(1063244800, 1094780800)

    def changeSystemConfiguration(self):
        self.myCursor.execute("ALTER SYSTEM SET max_connections = '200' ")
        self.myCursor.execute("ALTER SYSTEM SET shared_buffers = '2GB' ")
        self.myCursor.execute("ALTER SYSTEM SET effective_cache_size = '6GB' ")
        self.myCursor.execute("ALTER SYSTEM SET maintenance_work_mem = '512MB' ")
        self.myCursor.execute("ALTER SYSTEM SET checkpoint_completion_target = '0.9' ")
        self.myCursor.execute("ALTER SYSTEM SET wal_buffers = '16MB' ")
        self.myCursor.execute("ALTER SYSTEM SET default_statistics_target = '100' ")
        self.myCursor.execute("ALTER SYSTEM SET random_page_cost = '1.1'")
        self.myCursor.execute("ALTER SYSTEM SET work_mem = '5242kB' ")
        self.myCursor.execute("ALTER SYSTEM SET min_wal_size = '1GB' ")
        self.myCursor.execute("ALTER SYSTEM SET max_wal_size = '4GB' ")

postGresTest = PostGreSQLDatabase("127.0.0.1","postgres","mysecretpassword","postgres")
postGresTest.insertComet("/Users/rubenverma/Downloads/1002378")
