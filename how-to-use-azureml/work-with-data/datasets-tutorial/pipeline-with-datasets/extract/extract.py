# %%
import argparse
import datetime
import os
import time
import re
from pprint import pprint

import numpy as np
import pandas as pd
from azureml.core import Run
from azureml.core.authentication import InteractiveLoginAuthentication
from azureml.core.dataset import Dataset
from azureml.core.workspace import Workspace

def str_to_bool(value):
    """
    *Convert a string to a boolean

    *Args:
        *value (Str): The value to convert to a boolean

    *Returns:
        *vallue: True or False depending on value of input
    """
    if value.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif value.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def print_shape(df, msg):
    ts = time.time()
    ts_str = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    print(ts_str, msg, df.shape)

    return df


def main(output_dir, nrows, dataset):

    t0 = time.time()
    print("start_time", datetime.datetime.fromtimestamp(t0).strftime('%Y-%m-%d %H:%M:%S'))
    dataset_sample = dataset.take(nrows) if nrows is not None else dataset

    df_rawest = (dataset_sample.to_pandas_dataframe())

    t1 = time.time()
    print("__end_time", datetime.datetime.fromtimestamp(t1).strftime('%Y-%m-%d %H:%M:%S'))
    str_t1 = datetime.datetime.fromtimestamp(t1).strftime('%Y-%m-%d %H:%M:%S')
    elapsed = t1 - t0
    print("{} {} secs to read {} rows".format(str_t1, round(elapsed, 2), nrows))

    df_raw = (df_rawest
              .pipe(print_shape, 'initial shape:')
              )
    df_raw.to_csv(os.path.join(args.output_dir, "output.csv"), index=False)


if __name__ == "__main__":
    parser = (argparse.ArgumentParser(
        description="Starts the data transformation"))
    parser.add_argument('--input_dir',
                        dest='input_dir',
                        default="C:\\\\temp")
    parser.add_argument('--output_dir',
                        dest="output_dir",
                        default="C:\\\\temp\\output")
    parser.add_argument('--remote_run',
                        dest='remote_run',
                        type=str_to_bool,
                        default=False)
    parser.add_argument('--n_rows', dest="n_rows", type=int)

    args = parser.parse_args()
    print("all args:")
    pprint(vars(args))

    from azureml.core import VERSION
    print("Azure ML SDK Version: ", VERSION)
    from azureml.dataprep import __version__ as dp_version
    print("Azure ML dataprep Version: ", dp_version)

    run = Run.get_context()

    # create output directory if it does not exist
    run.log('output_dir', args.output_dir)
    os.makedirs(args.output_dir, exist_ok=True)

    if args.n_rows is not None:
        print("processing first", args.n_rows, "rows")
    else:
        print("is this none? nrows = ", type(args.n_rows))

    if args.remote_run is False:
        auth = InteractiveLoginAuthentication(tenant_id="cf36141c-ddd7-45a7-b073-111f66d0b30c")
        ws = Workspace.from_config(
            auth=auth,
            path="compute/aml_config/config.json")
        print("Found workspace {} at location {}".format(ws.name, ws.location))
        dataset = Dataset.get_by_name(workspace=ws, name='Diabetes')
    elif args.remote_run is True:
        dataset = run.input_datasets['is_there_under__score_limit']
    else:
        raise Exception('remote_run unknown value. The value was: {}'.format(args.remote_run))

    print("Using {} dataset version {}".format(dataset.name, dataset.version))

    main(args.output_dir, args.n_rows, dataset)


