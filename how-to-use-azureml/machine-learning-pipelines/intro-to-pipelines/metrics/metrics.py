import json
import os
from pprint import pprint

import pandas as pd
from azureml.core import Run


def df2csv(df, dir, filename, **kwargs):
    path = os.path.join(dir, filename)
    print("saving {} to {}".format(filename, dir))
    df.to_csv(path, index=False, **kwargs)
    return df


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', dest="input_file",
                        default="C://temp//output")
    parser.add_argument('--output_dir', dest="output_dir",
                        default="C://temp//output")
    # parser.add_argument('--exp_name', dest="exp_name",
    #                     default="attrition_more_countries")

    args = parser.parse_args()
    args_dict = dict(**vars(args))
    print("all args: ")
    pprint(vars(args))

    cwd = os.getcwd()
    os.makedirs(args.output_dir, exist_ok=True)

    run = Run.get_context()

    print("cwd:", cwd)
    print("dir of cwd", os.listdir(cwd))

    parent = os.path.dirname(args.input_file)
    print("input_dir_parent:", parent)
    print("dir of input_dir_parent:", os.listdir(parent))

    print("input file:", args.input_file)

    # SUGGESTED METHOD
    with open(args.input_file) as f:  
        metrics_output_result = f.read()
    
    deserialized_metrics_output = json.loads(metrics_output_result)
    df = (pd.DataFrame(deserialized_metrics_output)
          .pipe(df2csv, args.output_dir, "hyperdrive_metrics_OG.csv")
          .pipe(df2csv, "./outputs", "hyperdrive_metrics_OG.csv")
          )
    
    print(df.head())


    # AVANADE'S APPROACH
    df = (pd.read_json(args.input_file, orient='index')
          # convert columns to floats from single-item lists
          .transform(lambda x: x.apply(lambda y: y[0]))
          .reset_index()
          .pipe(df2csv, args.output_dir, "hyperdrive_metrics.csv")
          .pipe(df2csv, "./outputs", "hyperdrive_metrics.csv")
          )
    print(df.head())
