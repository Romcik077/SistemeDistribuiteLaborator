'''
Created on 21 nov. 2015

@author: Carolina
'''

import hashlib
import time
import multiprocessing
from time import sleep


path = '\\\\CAROLINA-PC\\working-dir\\'

def md5hashfile(fileName, blocksize=65536):
    hasher = hashlib.md5()
    afile = open(fileName, "rb")
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.digest()

cpuNum = multiprocessing.cpu_count()    
startTime=time.time()
print md5hashfile(path + "testFile.jpg", 4096)
endTime=time.time()
executionTime=endTime-startTime
print executionTime/cpuNum

testFile = open(path + "testFile", "w")
while 1:
    print "writing"
    testFile.seek(0)
    testFile.write("hello\n")
    testFile.flush()
    sleep(2)



