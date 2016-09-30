'''
Created on 21 nov. 2015

@author: Roman
'''

import os
import re
from time import time
from time import sleep

from multiprocessing import cpu_count
from multiprocessing import Process

from dircache import listdir

from json.encoder import JSONEncoder
from json.decoder import JSONDecoder

from hashlib import md5
from random import randint

workingFilesPath = '\\\\CAROLINA-PC\\tema3\\working-files\\'
resultPath = '\\\\CAROLINA-PC\\tema3\\result-log\\'

for afile in listdir(workingFilesPath):
    oldafile = afile
    if "ID" in afile:
        result = re.search(".ID\w+", afile)
        print result.group(0)
        afile = afile.replace(result.group(0), '')
    if '.done' in afile:
        afile = afile.replace('.done', '')
    if '.busy' in afile:
        afile = afile.replace('.busy', '')
    if ".ready" not in afile:
        afile = afile +'.ready'
        print afile
    os.rename(workingFilesPath+oldafile, workingFilesPath+afile)
    
    
    