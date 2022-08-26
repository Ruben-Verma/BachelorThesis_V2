import cProfile
import os
import mariadb
from timeit import default_timer as timer
import numpy as np

from DataBaseUtils import DataBaseUtils


class MariaDBDatabase:

    # Initialize MariaDBDatabase Object and creates
    # Cursor and Table to interact with the Database
    def __init__(self, host, port, user, password, database):
        """
        Initialize MariaDBDatabase Object and creates
        Cursor and Table to interact with the Database
        :param host: "127.0.0.1" or ip address of server
        :param port: Port of database usually 3306
        :param user: username of database
        :param password: password of database
        :param database: name of database
        """

        try:
            self.con = mariadb.connect(host=host, port=port, user=user, password=password, database=database)
        except mariadb.Error:
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
            try:
                self.myCursor.executemany(
                    "INSERT INTO ParticleStates(Cometid, Mass, ParticleNo,ParticleState_x,ParticleState_y,ParticleState_z,ParticleState_Vx,ParticleState_Vy,ParticleState_Vz,ETinSeconds) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    particle_state_list)
            except mariadb.InterfaceError:
                pass

            end = timer()
            self.myCursor.executemany(
                "INSERT INTO ParticleHeader(Cometid,Mass,ParticleNo,Multiplicationfactor) values (?,?,?,?)",
                ph_list)
            total_time += (end - start)
            print(end - start)
        print(total_time)

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
        Timespan is one year
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


testDatabase = MariaDBDatabase("127.0.0.1", 3306, "root", "my-secret-pw", "COMETS")
