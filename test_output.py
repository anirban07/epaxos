import sys

# Checks to ensure the reads in all the files passed in reflect the same value for all commands.
# Usage: python test_output.py [output_files...]

# Expects file to only have lines in the following format:
# 2019/06/10 23:26:44 Replica 1: Executed command {59 2 42 59} with seq: 80, result: 99

def add_results(filename):
    commandid_to_result = {}
    with open(filename) as file:
        line = file.readline()
        while line:
            line_arr = line.split(' ')
            # Sometimes the last line is an incomplete output
            if len(line_arr) != 15:
                line = file.readline()
                continue
            command_id = line_arr[6][1:]
            op_type = line_arr[7]
            result = line_arr[-1][:-1]
            if op_type == "2":
                if command_id in commandid_to_result:
                    print("file: " + filename + " has duplicate execution. Command ID: " + command_id)
                    exit(1)
                commandid_to_result[command_id] = result
            line = file.readline()
    return commandid_to_result

if __name__ == '__main__':
    output_filenames = sys.argv[1:]
    commandid_to_result = {}
    for output_filename in output_filenames:
        curr_commandid_to_result = add_results(output_filename)
        for command_id, value in curr_commandid_to_result.items():
            if command_id in commandid_to_result:
                if commandid_to_result[command_id] != value:
                    print("ERROR CommandId: " + command_id)
            else:
                commandid_to_result[command_id] = value
