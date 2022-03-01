import io
import os

import psycopg2
from timeit import default_timer as timer
import numpy as np
import psycopg2.extras
from DataBaseUtils import DataBaseUtils
import spiceypy as spice
import cProfile


class PostGreSQLDatabase:
    # Initialize MariaDBDatabase Object and creates
    # Cursor and Table to interact with the Database
    def __init__(self, host, user, password, database):
        try:
            self.con = psycopg2.connect(host=host, user=user, password=password, database=database)
            self.con.autocommit = True
        except psycopg2.Error:
            print("Datenbank konnte nicht geladen werden")
        self.myCursor = self.con.cursor()
        self.create_table_structure()

    def create_table_structure(self):
        try:
            self.myCursor.execute("""CREATE TABLE Population(
                Cometid integer,
                Mass FLOAT,
                Beta FLOAT,
                MaxTimeDifference FlOAT
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
        except psycopg2.DatabaseError:
            pass

    # Searches particle given a timespan time1 - time 2
    def search_particle(self, time1, time2):
        start = timer()
        self.myCursor.execute(
            "SELECT * FROM particlestates WHERE ETinSeconds BETWEEN ? AND ? ORDER BY ETinSeconds ASC",
            (time1, time2))
        result = self.myCursor.fetchall()
        end = timer()
        print(end - start)
        return result

    # Inserts data into the table using parameter lists and csv buffer to increase insert performance
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

            buff = DataBaseUtils.tuplelist_into_csv_buffer(particle_state_list)

            self.myCursor.copy_from(buff, "particlestates", sep="|", columns=(
                "cometid", "mass", "particleno", "particlestate_x", "particlestate_y", "particlestate_z",
                "particlestate_vx",
                "particlestate_vy", "particlestate_vz", "etinseconds"))
            end = timer()
            particle_header_list.extend(ph_list)
            total_time += (end - start)
            print(end - start)
        print(total_time)
        psycopg2.extras.execute_batch(self.myCursor, "Insert into particleheader values (%s,%s,%s,%s)",
                                      particle_header_list)
        psycopg2.extras.execute_batch(self.myCursor, "Insert into population values (%s,%s,%s,%s)",
                                      [(comet_id, i, beta_value_list[i], maximum_time_difference_list[i].item()) for i
                                       in range(0, len(maximum_time_difference_list))])

    # Defines a testcase how fast Database retrieves Data when queries overlap
    # Timespan is one year
    def one_year_testcase(self):
        self.search_particle(1000000000, 1031536000)
        self.search_particle(1000172800, 1031708800)

    # Same semantics as oneYearTestcase Method but timespan are two years
    def two_year_testcase(self):
        self.search_particle(1000000000, 1063072000)
        self.search_particle(1063244800, 1094780800)


postGresTest = PostGreSQLDatabase("127.0.0.1", "postgres", "mysecretpassword", "postgres")

cProfile.run("""postGresTest.insert_comet("/Users/rubenverma/Documents/Bachelorarbeit/1002378")""")
