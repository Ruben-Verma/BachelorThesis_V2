import csv
import math
import io

import numpy
import spiceypy as spice
from timeit import default_timer as timer


class DataBaseUtils:

    @staticmethod
    def tuple_list_maker(byte_array):
        """
        deprecated method but not deleted until bachelorthesis ends
        :param byte_array:
        :return:
        """
        tuple_list = []

        for i in range(0, len(byte_array), 7):
            tuple_list.append((byte_array[i].item(), byte_array[i + 1].item(),
                               byte_array[i + 2].item(), byte_array[i + 3].item(),
                               byte_array[i + 4].item(), byte_array[i + 5].item(),
                               byte_array[i + 6].item()))
        return tuple_list

    @staticmethod
    def tuple_list_maker_structure(byte_array, comet_id, mass):
        """
        :param byte_array: Bytearray from Workunit file
        :param comet_id: Id of comet
        :param mass: mass of Workunit file
        :return: List of particle States for Database Insertion
        """
        particle_no = 0
        particle_state_values = []
        particle_header_values = []
        continued = False
        for i in range(7, len(byte_array), 7):
            if continued:
                continued = False
                continue
            if (byte_array[i] == 0):
                particle_no = byte_array[i + 4]
                particle_header_values.append((comet_id, mass,
                                               byte_array[i + 4].item(), byte_array[i + 5].item()))
                continued = True
                continue
            try:
                if ((particle_state_values[-1][9] - byte_array[i + 6]) != 0.0):
                    particle_state_values.append((comet_id, mass, int(particle_no.item()),
                                                  byte_array[i].item(), byte_array[i + 1].item(),
                                                  byte_array[i + 2].item(), byte_array[i + 3].item(),
                                                  byte_array[i + 4].item(), byte_array[i + 5].item(),
                                                  byte_array[i + 6].item()))
            except IndexError:
                print("liste leer")
                particle_state_values.append((comet_id, mass, int(particle_no.item()),
                                              byte_array[i].item(), byte_array[i + 1].item(),
                                              byte_array[i + 2].item(), byte_array[i + 3].item(),
                                              byte_array[i + 4].item(), byte_array[i + 5].item(),
                                              byte_array[i + 6].item()))

        return particle_header_values, particle_state_values

    @staticmethod
    def sql_string_maker_structure(byte_array, comet_id, mass):
        """
        deprecated method but not deleted until bachelorthesis ends
        :param byte_array:
        :return:
        """
        particle_no = 0
        sql_string_particle_state = []
        sql_string_particle_header = []
        for i in range(7, len(byte_array), 7):
            if (byte_array[i] == 0):
                particle_no = byte_array[i + 4]
                sql_string_particle_header.append("(")
                sql_string_particle_header.append(str(comet_id))
                sql_string_particle_header.append(",")
                sql_string_particle_header.append(str(mass))
                sql_string_particle_header.append(",")
                sql_string_particle_header.append(str(byte_array[i + 4]))
                sql_string_particle_header.append(",")
                sql_string_particle_header.append(str(byte_array[i + 5]))
                sql_string_particle_header.append(")")
                sql_string_particle_header.append(",")
                continue

            sql_string_particle_state.append("(")
            sql_string_particle_state.append(str(comet_id))
            sql_string_particle_state.append(",")
            sql_string_particle_state.append(str(mass))
            sql_string_particle_state.append(",")
            sql_string_particle_state.append(str(particle_no))
            sql_string_particle_state.append(",")
            sql_string_particle_state.append(str(byte_array[i]))
            sql_string_particle_state.append(",")
            sql_string_particle_state.append(str(byte_array[i + 1]))
            sql_string_particle_state.append(",")
            sql_string_particle_state.append(str(byte_array[i + 2]))
            sql_string_particle_state.append(",")
            sql_string_particle_state.append(str(byte_array[i + 3]))
            sql_string_particle_state.append(",")
            sql_string_particle_state.append(str(byte_array[i + 4]))
            sql_string_particle_state.append(",")
            sql_string_particle_state.append(str(byte_array[i + 5]))
            sql_string_particle_state.append(",")
            sql_string_particle_state.append(str(byte_array[i + 6]))
            sql_string_particle_state.append(")")
            sql_string_particle_state.append(",")
        return sql_string_particle_header, "".join(sql_string_particle_state)

    @staticmethod
    def mass_to_int(mass):
        """
        :param mass: mass from Workunit File
        :return: The corresponding Integer for it's mass
        """
        if math.isclose(1.000000e-08, mass, abs_tol=3.1e-09):
            return 0
        if math.isclose(1.640000e-08, mass, abs_tol=1e-09):
            return 1
        if math.isclose(4.390000e-08, mass, abs_tol=1e-08):
            return 2
        if math.isclose(1.930000e-07, mass, abs_tol=1e-08):
            return 3
        if math.isclose(1.390000e-06, mass, abs_tol=1e-07):
            return 4
        if math.isclose(1.640000e-05, mass, abs_tol=1e-06):
            return 5
        if math.isclose(3.160000e-04, mass, abs_tol=1e-05):
            return 6
        if math.isclose(1.000000e-02, mass, abs_tol=1e-03):
            return 7

    @staticmethod
    def tuplelist_into_csv_buffer(list):
        """
        :param list:List of Particle States
        :return: In-file CSV object for insertion for Postgresql
        """
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter="|", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(list)
        buffer.seek(0)
        return buffer

    @staticmethod
    def calculate_maximum_time_difference(byte_array, time_difference_list, mass):
        """
        :param byte_array: Bytearray from Workunitfile
        :param time_difference_list: List of Timedifferences between Particles States for each mass
        :param mass: mass of workunit file
        :return: Updated maximum time difference list
        """
        continued = False
        for i in range(7, len(byte_array) - 7, 7):
            if continued:
                continued = False
                continue
            if byte_array[i] == 0:
                continued = True
                continue
            if (byte_array[i + 13] - byte_array[i + 6]) > time_difference_list[mass]:
                time_difference_list[mass] = (byte_array[i + 13] - byte_array[i + 6])

    @staticmethod
    def calculate_nearest_particles(particle_list, time):
        """
        :param particle_list: Lists of particles from the previous query
        :param time: input time
        :return: List of all particles which are the nearest in the given time
        """
        updated_particle_list = []
        previous_particle_mass = particle_list[0][1]  # Preloades the variables for the loop
        previous_particle_number = particle_list[0][2]
        min_difference_particle = particle_list[0]
        min_time_difference_between_particle = particle_list[0][9]

        for particle in particle_list:
            if particle[1] != previous_particle_mass or particle[2] != previous_particle_number:  # checks for changing particle mass or Number
                updated_particle_list.append(min_difference_particle)  # appends the previous particle in list and preloads the variables again
                previous_particle_mass = particle[1]
                previous_particle_number = particle[2]
                min_difference_particle = particle
                min_time_difference_between_particle = particle[9]
            if abs(particle[9] - time) < min_time_difference_between_particle and particle[9] <= time:  # changes the minimal particle if the new particle is nearer the given time
                min_difference_particle = particle
                min_time_difference_between_particle = abs(particle[9] - time)
        updated_particle_list.append(min_difference_particle)  # Very last particle must be inserted manually
        return updated_particle_list

    @staticmethod
    def calculate_spice_extrapolation(particles, time):
        """
        :param particles: list of particles which needs to be extrapolated
        :param time: input time
        :return: extrapolated particle in the given time
        """
        extrapolation_list = []
        start = timer()  # used for spice calculation
        mu_sun = 132712440023.31
        for particle in particles:  # extrapolates each particles and appends to return list
            spice_array = particle[3:9]
            orbital_elements = spice.oscelt(spice_array, particle[9], mu_sun)
            state = spice.conics(orbital_elements, time)
            extrapolation_list.append(state)
        end = timer()
        print(end - start)
        return extrapolation_list

    @staticmethod
    def calculate_spice_test():
        """

        :return:
        """
        spice.furnsh("/Users/rubenverma/Downloads/meta_kernel.txt")
        et = math.trunc(spice.str2et("2024 December 14, 22:00:00"))
        state, lt = spice.spkezr("EARTH", et, "ECLIPJ2000", "NONE", "SSB")
        print("Earth State Vector:")
        print(state)
        # Todo: compare state Vektor with future comet state vektor after spice calculation after Bachelorthesis
