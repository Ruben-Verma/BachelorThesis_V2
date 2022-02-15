import os
import sqlite3
from timeit import default_timer as timer
import numpy as np
from DataBaseUtils import DataBaseUtils


class SQLiteDatabase:

    # Initialize SQLiteDatabase Object and creates
    # Cursor and Table to interact with the Database
    def __init__(self, database_name):
        self.con = sqlite3.connect(database_name + '.db', isolation_level='DEFERRED')
        self.myCursor = self.con.cursor()
        self.create_table()

    # Creates SQL Table
    def create_table(self):
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
        except sqlite3.Error:
            pass

    # Insert Comet into the table
    def inser_comet(self, path):
        total_time = 0
        comet_number = 1
        file_list = []
        with os.scandir(path) as it:
            for entry in it:
                if entry.name.endswith(".ctwu") and entry.is_file():
                    file_list.append(entry.path)  # Collects all Workunit file paths that will be inserted

        file_list.sort()

        for path in file_list:
            start = timer()
            with open(path, 'rb') as file:
                float_values = np.array(np.fromfile(file, dtype=np.float32))
            s = DataBaseUtils.tuple_list_maker(float_values)
            end = timer()
            print(comet_number)
            comet_number = comet_number + 1
            self.myCursor.executemany(
                "INSERT INTO COMETS(ParticleState_x,ParticleState_y,ParticleState_z,ParticleState_Vx,ParticleState_Vy,ParticleState_Vz,ETinSeconds) VALUES(?,?,?,?,?,?,?)",
                s)
            self.con.commit()
            end = timer()
            total_time += (end - start)
            print(end - start)
        print(total_time)

    # Searches particle given a timespan time1 - time 2
    def search_particle(self, time1, time2):
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
    def one_year_testcase(self):
        self.search_particle(1000000000, 1031536000)
        self.search_particle(1000172800, 1031708800)

    # Same semantics as oneYearTestcase Method but timespan are two years
    def two_year_testcase(self):
        self.search_particle(1000000000, 1063072000)
        self.search_particle(1063244800, 1094780800)
