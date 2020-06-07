import sys
from pyspark.sql import SparkSession
from datetime import datetime
from common_module import read_config, run_cmd


def load_config_json(input_json_path):
    global insert_mode
    global hdfs_src_path
    global content_type
    global this_month
    global targetDB

    attributes = read_config(input_json_path)
    insert_mode = attributes.iloc[0].insert_mode
    hdfs_src_path = attributes.iloc[0].hdfs_src_path
    content_type = attributes.iloc[0].content_type
    targetDB = attributes.iloc[0].targetDB
    this_month = datetime.strftime(datetime.now(), '%Y-%m')


def run_mapping(spark, mapping_json_path):
    key_values = read_config(mapping_json_path)
    for table_name in key_values:
        print(table_name)
        for j in key_values[table_name]:
            for src_name in j:
                print(j[src_name])

                (ret, out, err) = run_cmd(['hadoop', 'fs', 'ls'],
                                          hdfs_src_path + this_month + str("/*/") + src_name + str("*"))


def main():
    try:
        input_json_path = str(sys.argv[1])
        mapping_json_path = str(sys.argv[2])
    except Exception as ex1:
        print("Json file not found %s" % ex1)

        try:
            spark = SparkSession.builder.appName("Spark Program").getOrCreate
            spark.sparkContext.setLogLevel("ERROR")
        except Exception as ex2:
            print("Issue in spark session %s" % ex2)
            sys.exit(2)

            try:
                run_mapping(spark, mapping_json_path)
            except Exception:
                raise

            try:
                main()
            except Exception as ex:
                return_code = 1
                sys.exit(return_code)
            else:
                return_code = 0
                sys.exit(return_code)
