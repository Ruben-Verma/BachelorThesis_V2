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
        """
        initialize connection object of postgres database
        :param host: "127.0.0.1" or ip address of server
        :param user: user name of database
        :param password: password of database
        :param database: name of database
        """
        try:
            self.con = psycopg2.connect(host=host, user=user, password=password, database=database)
            self.con.autocommit = True
        except psycopg2.Error:
            print("Datenbank konnte nicht geladen werden")
        self.myCursor = self.con.cursor()
        self.create_table_structure()

    def create_table_structure(self):
        """
        Creates table structure for database for converted Workunit files with
        Population, Particle Header and Particle States
        """
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
             ) Partition by range (ETinSeconds)
                  """)
            self.myCursor.execute(
                """Create Table Particle1980to1990 Partition of ParticleStates for values from (-632910000) to (-316454999)""")
            self.myCursor.execute(
                """Create Table Particle1990to2000 Partition of ParticleStates for values from (-316454998) to (-1)""")
            self.myCursor.execute(
                """Create Table Particle2000to2010 Partition of ParticleStates for values from (0) to (316454999)""")
            self.myCursor.execute(
                """Create Table Particle2010to2020 Partition of ParticleStates for values from (316455000) to (632909999)""")
            self.myCursor.execute(
                """Create Table Particle2020to2030 Partition of ParticleStates for values from (632910000) to (949364999)""")
            self.myCursor.execute(
                """Create Table Particle2030to2040 Partition of ParticleStates for values from (949365000) to (1265819999)""")
            self.myCursor.execute(
                """Create Table Particle2040to2050 Partition of ParticleStates for values from (1265820000) to (1582274999)""")
            self.myCursor.execute(
                """Create Table Particle2050to2060 Partition of ParticleStates for values from (1582275000) to (1898729999)""")
            self.myCursor.execute(
                """Create Table Particle2060to2070 Partition of ParticleStates for values from (1898730000) to (2215184999)""")
            self.myCursor.execute(
                """Create Table Particle2070to2080 Partition of ParticleStates for values from (2215185000) to (2531640000)""")
        except psycopg2.DatabaseError as e:
            print(e)

    def search_particle(self, time1, time2):
        """
        :param time1: time for oldest particle
        :param time2: time for youngest particle
        :return: all particles between time1 and time2
        """
        start = timer()
        self.myCursor.execute(
            "SELECT * FROM particlestates WHERE ETinSeconds BETWEEN %s AND %s ORDER BY Mass,ParticleNo",
            (time1, time2))
        result = self.myCursor.fetchall()
        end = timer()
        print(end - start)
        return result

    def insert_comet(self, path):
        """
        Inserts data into the table using parameter lists
        and csv buffer to increase insert performance
        :param path: filepath for comet
        """
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

    def one_year_testcase(self):
        """
        Defines a testcase how fast Database retrieves Data when queries overlap
        Timespan is one year
        """
        self.search_particle(1000000000, 1031536000)
        self.search_particle(1000172800, 1031708800)

    def two_year_testcase(self):
        """
        Same semantics as oneYearTestcase Method but timespan are two years
        """
        self.search_particle(1000000000, 1063072000)
        self.search_particle(1063244800, 1094780800)

    def particle_analyzer_spice(self, time):
        """
        :param time: Time for retrieval
        :return: Extrapolated particles regarding the input time
        """
        self.myCursor.execute("SELECT MAX(MaxTimeDifference) FROM Population")
        max_time_difference = self.myCursor.fetchall()[0][0]

        state_list = self.search_particle(max(time - max_time_difference, 0), time)
        particles = DataBaseUtils.calculate_nearest_particles(state_list, time)
        return DataBaseUtils.calculate_spice_extrapolation(particles, time)


postGresTest = PostGreSQLDatabase("127.0.0.1", "postgres", "mysecretpassword", "postgres")
postGresTest.particle_analyzer_spice(787485669)
