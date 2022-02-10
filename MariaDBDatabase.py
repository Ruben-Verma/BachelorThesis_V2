import os
import mariadb
from timeit import default_timer as timer
import numpy as np

class MariaDBDatabase:

    # Initialize MariaDBDatabase Object and creates
    # Cursor and Table to interact with the Database
    def __init__(self,host,port,user,password,database):
        try:
            self.con = mariadb.connect(host = host,port=port,user=user,password=password,database = database)
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
    # Creates Inputlist for the insertion into the table
    def tupleListMaker(self,byteArray):
        tupleList = []

        for i in range(0, len(byteArray), 7):
            tupleList.append((byteArray[i].item(), byteArray[i + 1].item(),
                              byteArray[i + 2].item(), byteArray[i + 3].item(),
                              byteArray[i + 4].item(), byteArray[i + 5].item(),
                              byteArray[i + 6].item()))
        return tupleList

    # Insert Comet into the table
    def inserComet(self,path):
        totalTime = 0
        cometNumber = 1
        fileList = []
        with os.scandir(path) as it:
            for entry in it:
                if entry.name.endswith(".ctwu") and entry.is_file():
                    fileList.append(entry.path) # Collects all Workunit file paths that will be inserted

        fileList.sort()

        for path in fileList:
            start = timer()
            with open(path, 'rb') as file:
                floatValues = np.array(np.fromfile(file, dtype=np.float32))
            s = self.tupleListMaker(floatValues)
            end = timer()
            print(cometNumber)
            cometNumber = cometNumber + 1
            self.myCursor.executemany(
                "INSERT INTO COMETS(ParticleState_x,ParticleState_y,ParticleState_z,ParticleState_Vx,ParticleState_Vy,ParticleState_Vz,ETinSeconds) VALUES(?,?,?,?,?,?,?)",
                s)
            self.con.commit()
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

#databaseMariaTest = MariaDBDatabase("127.0.0.1",3306,"root","my-secret-pw","COMETS")
#databaseMariaTest.inserComet("/Users/rubenverma/Downloads/1002378")
