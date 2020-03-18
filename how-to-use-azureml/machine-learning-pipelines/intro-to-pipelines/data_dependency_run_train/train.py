# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license.

import argparse
import os
from azureml.core import Run

print("In train.py")
print("As a data scientist, this is where I use my training code.")

parser = argparse.ArgumentParser("train")

parser.add_argument("--input_data", type=str, help="input data")
parser.add_argument("--output_train", type=str, help="output_train directory")

args = parser.parse_args()

print("Argument 1: %s" % args.input_data)
print("Argument 2: %s" % args.output_train)

run= Run.get_context()
print("tag locally")
run.tag('arg1',args.input_data)
print("tag remote")
run.parent.tag('arg1',args.input_data)

if not (args.output_train is None):
    os.makedirs(args.output_train, exist_ok=True)
    print("%s created" % args.output_train)
