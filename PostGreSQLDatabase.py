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
        except psycopg2.Error:
            print("Datenbank konnte nicht geladen werden")
        self.myCursor = self.con.cursor()
        self.create_table()

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
        except psycopg2.Error:
            pass

    # Create Insert String for the SQL statement
    def sql_string_maker(self, byte_array):
        sql_string2 = []
        for i in range(0, len(byte_array), 7):
            sql_string2.append("(")
            sql_string2.append(str(byte_array[i]))
            sql_string2.append(",")
            sql_string2.append(str(byte_array[i + 1]))
            sql_string2.append(",")
            sql_string2.append(str(byte_array[i + 2]))
            sql_string2.append(",")
            sql_string2.append(str(byte_array[i + 3]))
            sql_string2.append(",")
            sql_string2.append(str(byte_array[i + 4]))
            sql_string2.append(",")
            sql_string2.append(str(byte_array[i + 5]))
            sql_string2.append(",")
            sql_string2.append(str(byte_array[i + 6]))
            sql_string2.append(")")
            sql_string2.append(",")
        return "".join(sql_string2)

    def insert_comet(self, path):
        total_time = 0
        comet_number = 1
        file_list = []
        with os.scandir(path) as it:
            for entry in it:
                if entry.name.endswith(".ctwu") and entry.is_file():
                    file_list.append(entry.path)

        file_list.sort()

        for path in file_list:
            start = timer()
            with open(path, 'rb') as file:
                float_values = np.array(np.fromfile(file, dtype=np.float32))
            s = self.sql_string_maker(float_values)
            end = timer()
            print(comet_number)
            comet_number = comet_number + 1
            self.myCursor.execute("INSERT INTO COMETS VALUES  " + s[0:-1])
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
postGresTest.insert_comet("/Users/rubenverma/Downloads/1002378")
