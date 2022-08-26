import cProfile
import os
import sqlite3
from timeit import default_timer as timer
import numpy as np
from matplotlib import pyplot as plt

from DataBaseUtils import DataBaseUtils


class SQLiteDatabase:

    # Initialize SQLiteDatabase Object and creates
    # Cursor and Table to interact with the Database
    def __init__(self, database_name):
        """
        initializes database with given name
        :param database_name:
        """
        self.con = sqlite3.connect(database_name + '.db', isolation_level='DEFERRED')
        self.myCursor = self.con.cursor()
        self.create_table_structure()

    def create_table_structure(self):
        """
        Creates table structure for database
        """
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
        except sqlite3.OperationalError:
            pass

    def insert_comet(self, path):
        """
        :param path: Path of Comet file
        Inserts one Comet into the Database with 3 different tables
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

    def search_particle(self, time1, time2):
        """
        :param time1: time for oldest particle
        :param time2: time for youngest particle
        :return: all particles between time1 and time2
        """
        start = timer()
        self.myCursor.execute(
            "SELECT * FROM ParticleStates WHERE ETinSeconds BETWEEN ? AND ? ORDER BY Mass,ParticleNo",
            (time1, time2))
        result = self.myCursor.fetchall()
        end = timer()
        print(end - start)
        return result

    def one_year_testcase(self):
        """
        Defines a testcase how fast Database retrieves Data when queries overlap
        Timespan is one year
        """
        self.search_particle(1000000000, 1031536000)
        self.search_particle(1000172800, 1031708800)

    def two_year_testcase(self):
        """
        Defines a testcase how fast Database retrieves Data when queries overlap
        Timespan is two years
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

    def multiple_particle_analyzer_spice(self, times):
        """
        :param time: Multiple Timestamps for retrieval
        :return: Extrapolated particles regarding the input times
        """
        sorted(times)
        result_list = []
        extrapolation_list = []
        self.myCursor.execute("SELECT MAX(MaxTimeDifference) FROM Population")

        max_time_difference = self.myCursor.fetchall()[0][0]
        state_list = self.search_particle(times[0] - max_time_difference, times[-1])
        sorted(state_list, key=lambda x: x[9])
        i = 0
        for time in times:
            result_list.append(DataBaseUtils.calculate_nearest_particles_multiple(state_list, time, max_time_difference))
            print(len(result_list[i]))
            i = i+1

        i = 0
        for time in times:
            extrapolation_list.append(DataBaseUtils.calculate_spice_extrapolation(result_list[i], time))

timestamps = [787485669, 787485679, 787485689, 787485699, 787485709, 787485719, 787485729, 787485739,
              787485749, 787485759, 787485769, 787485779, 787485789, 787485799, 787485809, 787485819,
              787485829, 787485839, 787485849, 787485859, 787485869, 787485879, 787485889, 787485899,
              787485909, 787485919, 787485929, 787485939, 787485949, 787485959]

sqlitetest = SQLiteDatabase("ruben")
sqlitetest.myCursor.execute("SELECT MAX(MaxTimeDifference) FROM Population")
max_time_difference = sqlitetest.myCursor.fetchall()[0][0]
sqlitetest.multiple_particle_analyzer_spice(timestamps)
