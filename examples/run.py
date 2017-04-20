#! /usr/bin/python3

import glob
import os
import sys

cps = ":".join(glob.glob("../jars/*.jar"))
args = list(sys.argv[1:])
print(args)
print("kotlin -cp {cps}:samples.jar SampleRsvKt {args}".format(cps=cps, args=" ".join(args) ))
os.system( "kotlin -cp {cps}:samples.jar SampleRsvKt {args}".format(cps=cps, args=" ".join(args)) )
