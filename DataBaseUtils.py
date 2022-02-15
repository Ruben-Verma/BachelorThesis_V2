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
