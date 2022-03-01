import csv
import math
import io


class DataBaseUtils:
    # Creates Inputlist for the insertion into the table
    @staticmethod
    def tuple_list_maker(byte_array):
        tuple_list = []

        for i in range(0, len(byte_array), 7):
            tuple_list.append((byte_array[i].item(), byte_array[i + 1].item(),
                               byte_array[i + 2].item(), byte_array[i + 3].item(),
                               byte_array[i + 4].item(), byte_array[i + 5].item(),
                               byte_array[i + 6].item()))
        return tuple_list

    @staticmethod
    def tuple_list_maker_structure(byte_array, comet_id, mass):
        particle_no = 0
        particle_state_values = []
        particle_header_values = []

        for i in range(7, len(byte_array), 7):
            if (byte_array[i] == 0):
                particle_no = byte_array[i + 4]
                particle_header_values.append((comet_id, mass,
                                               byte_array[i + 4].item(), byte_array[i + 5].item()))
                continue

            particle_state_values.append((comet_id, mass, int(particle_no.item()),
                                          byte_array[i].item(), byte_array[i + 1].item(),
                                          byte_array[i + 2].item(), byte_array[i + 3].item(),
                                          byte_array[i + 4].item(), byte_array[i + 5].item(),
                                          byte_array[i + 6].item()))

        return particle_header_values, particle_state_values

    @staticmethod
    def sql_string_maker_structure(byte_array, comet_id, mass):
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

    # Changes Postgres Server configuration to increase performance
    # This configuration suits the Macbook Pro 13' 2016
    @staticmethod
    def postgresql_change_system_configuration(postgres_database_object):
        postgres_database_object.myCursor.execute("ALTER SYSTEM SET max_connections = '200' ")
        postgres_database_object.myCursor.execute("ALTER SYSTEM SET shared_buffers = '2GB' ")
        postgres_database_object.myCursor.execute("ALTER SYSTEM SET effective_cache_size = '6GB' ")
        postgres_database_object.myCursor.execute("ALTER SYSTEM SET maintenance_work_mem = '512MB' ")
        postgres_database_object.myCursor.execute("ALTER SYSTEM SET checkpoint_completion_target = '0.9' ")
        postgres_database_object.myCursor.execute("ALTER SYSTEM SET wal_buffers = '16MB' ")
        postgres_database_object.myCursor.execute("ALTER SYSTEM SET default_statistics_target = '100' ")
        postgres_database_object.myCursor.execute("ALTER SYSTEM SET random_page_cost = '1.1'")
        postgres_database_object.myCursor.execute("ALTER SYSTEM SET work_mem = '5242kB' ")
        postgres_database_object.myCursor.execute("ALTER SYSTEM SET min_wal_size = '1GB' ")
        postgres_database_object.myCursor.execute("ALTER SYSTEM SET max_wal_size = '4GB' ")

    @staticmethod
    def mass_to_int(mass):
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
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter="|", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(list)
        buffer.seek(0)
        return buffer

    @staticmethod
    def calculate_maximum_time_difference(byte_array, time_difference_list, mass):
        for i in range(7, len(byte_array) - 7, 7):
            if byte_array[i] == 0:
                continue
            if (byte_array[i + 13] - byte_array[i + 6]) > time_difference_list[mass]:
                time_difference_list[mass] = (byte_array[i + 7] - byte_array[i])
