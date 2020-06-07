import pandas as pd
import subprocess
from subprocess import Popen, PIPE


def read_config(path):
    file_obj = open(path, "r")
    content = file_obj.read()
    file_obj.close()
    attributes = pd.read_json(content)
    return attributes


def run_cmd(arg_list):
    process = Popen(arg_list, stdout=PIPE, stderr=PIPE)
    s_out, s_err = process.communicate()
    s_return = process.returncode
    return s_return, s_out, s_err


def run_mapping(mapping_json_path):
    key_values = read_config(mapping_json_path)
    for table_name in key_values:
        print(table_name)
        for j in key_values[table_name]:
            for src_name in j:
                print(j[src_name])


if __name__ == '__main__':
    run_mapping("/Users/abhishekpolusani/PycharmProjects/pyspark/pyspark-project/scripts/mapping.json")