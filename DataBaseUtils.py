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
