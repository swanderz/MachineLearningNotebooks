import findspark as fs
from pprint import pprint
import numpy as np
import pyspark
import os
import argparse

import codecs  # noqa: F401
from codecs import open  # noqa: F401


def get_query_string(script_path):

    with open(script_path, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()
        query_string = " ".join(lines)
    return query_string


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


parser = argparse.ArgumentParser(description="Starts the data transformation")
parser.add_argument('--input_dir', dest="input_dir",
                    default="C:\\\\temp\\output")
parser.add_argument('--script_dir', dest="script_dir",
                    default="compute")
parser.add_argument('--output_dir', dest="output_dir",
                    default="C:\\\\temp\\output")

args = parser.parse_args()
print("all args:")
pprint(vars(args))

print('output_dir: {}'.format(args.output_dir))
os.makedirs(args.output_dir, exist_ok=True)


print("reading queries")
query_groupby = get_query_string(os.path.join(args.script_dir, 'groupby.sql'))

print("find spark")
fs.init()
print("Connect to Spark")
# start Spark session
spark = pyspark.sql.SparkSession.builder.appName('iris').getOrCreate()
print("spark con", spark)

print("reading TSVs")
file_location = os.path.join(args.input_dir, 'iris.csv')

df = spark \
    .read.format("com.databricks.spark.csv") \
    .option("delimiter", "\,") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(file_location)

df.printSchema()
df.createOrReplaceTempView("iris")
print("roll columns")

df = spark.sql(query_groupby)
df.createOrReplaceTempView("iris_summary")

df_roll = spark.table("iris_summary").toPandas()

print(df_roll.dtypes)

print("size with NaNs", df_roll.shape)
df_roll.to_csv(os.path.join(args.output_dir,
                            'irisGold_wNANs.tsv'), sep='\t', index=False)

df_roll = (df_roll
           # remove rows with inf nan or null values
           .replace([np.inf, -np.inf], np.nan)
           .dropna(axis=0)
           # sort columns by number of underscores in the column
           #  .pipe(SortUnderscores)
           )
print("size w/o NaNs", df_roll.shape)
print("saving to TSV")
df_roll.to_csv(os.path.join(args.output_dir,
                            'irisGold.tsv'), sep='\t', index=False)
