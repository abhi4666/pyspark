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


