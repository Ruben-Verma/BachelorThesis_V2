import os
import mariadb
from timeit import default_timer as timer
import numpy as np

from DataBaseUtils import DataBaseUtils


class MariaDBDatabase:

    # Initialize MariaDBDatabase Object and creates
    # Cursor and Table to interact with the Database
    def __init__(self, host, port, user, password, database):
        try:
            self.con = mariadb.connect(host=host, port=port, user=user, password=password, database=database)
        except mariadb.Error:
            print("Datenbank konnte nicht geladen werden")
        self.myCursor = self.con.cursor()
        self.create_table_structure()

    def create_table_structure(self):
        try:
            self.myCursor.execute("""CREATE TABLE Population(
                Cometid integer,
                Mass FLOAT,
                Beta FLOAT,
                MaxTimeDifference FLOAT
                )""")

            self.myCursor.execute("""
            CREATE TABLE ParticleHeader(
                Cometid integer,
                Mass FLOAT,
                ParticleNo INTEGER,
                Multiplicationfactor FLOAT
                )""")

            self.myCursor.execute("""CREATE TABLE ParticleStates(
                Cometid integer,
                Mass FLOAT,
                ParticleNo INTEGER,
                ParticleState_x FLOAT,
                ParticleState_y FLOAT,
                ParticleState_z FLOAT,
                ParticleState_Vx FLOAT,
                ParticleState_Vy FLOAT,
                ParticleState_Vz FLOAT,
                ETinSeconds FLOAT
             )
                  """)
        except mariadb.OperationalError:
            pass

    def insert_comet(self, path):
        particle_header_list = []
        maximum_time_difference_list = [0, 0, 0, 0, 0, 0, 0, 0]
        beta_value_list = [0, 0, 0, 0, 0, 0, 0, 0]

        total_time = 0
        comet_number = 1
        comet_id = path[len(path) - 7:len(path)]
        file_list = []

        with os.scandir(path) as it:
            for entry in it:
                if entry.name.endswith(".ctwu") and entry.is_file():
                    file_list.append(entry.path)

        file_list.sort()

        for path in file_list:
            with open(path, 'rb') as file:
                float_values = np.array(np.fromfile(file, dtype=np.float32))

            comet_mass = DataBaseUtils.mass_to_int(float_values[2])
            beta_value_list[comet_mass] = float_values[4].item()

            DataBaseUtils.calculate_maximum_time_difference(float_values, maximum_time_difference_list, comet_mass)

            start = timer()

            print(comet_number)

            comet_number = comet_number + 1
            ph_list, particle_state_list = DataBaseUtils.tuple_list_maker_structure(float_values, comet_id, comet_mass)

            self.myCursor.executemany(
                "INSERT INTO ParticleStates(Cometid, Mass, ParticleNo,ParticleState_x,ParticleState_y,ParticleState_z,ParticleState_Vx,ParticleState_Vy,ParticleState_Vz,ETinSeconds) VALUES(?,?,?,?,?,?,?,?,?,?)",
                particle_state_list)

            end = timer()
            particle_header_list.extend(ph_list)
            total_time += (end - start)
            print(end - start)
        print(total_time)
        self.myCursor.executemany(
            "INSERT INTO ParticleHeader(Cometid,Mass,ParticleNo,Multiplicationfactor) values (?,?,?,?)",
            particle_header_list)
        self.myCursor.executemany("INSERT INTO Population(Cometid,Mass,beta,MaxTimeDifference) values (?,?,?,?)",
                                  [(comet_id, i, beta_value_list[i], maximum_time_difference_list[i].item()) for i
                                   in range(0, len(maximum_time_difference_list))])
        self.con.commit()

    # Insert Comet into the table

    # Searches particle given a timespan time1 - time 2
    def search_particle(self, time1, time2):
        start = timer()
        self.myCursor.execute(
            "SELECT * FROM ParticleStates WHERE ETinSeconds BETWEEN ? AND ? ORDER BY ETinSeconds ASC",
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


testDatabase = MariaDBDatabase("127.0.0.1", 3306, "root", "my-secret-pw", "COMETS")

testDatabase.insert_comet("/Users/rubenverma/Documents/Bachelorarbeit/12345678")
