#! /usr/bin/python3

import glob
import os
import math

kts =  " ".join( glob.glob("*.kt") )
cps =  ":".join( glob.glob("../jars/*.jar") ) 

os.system("kotlinc {kts} -cp {cps} -include-runtime -d samples.jar".format(kts=kts, cps=cps))
