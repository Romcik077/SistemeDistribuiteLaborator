'''
Created on 21 nov. 2015

@author: Carolina
'''
import hashlib
import time
import multiprocessing
from time import sleep


path = '\\\\CAROLINA-PC\\working-dir\\'

def hashfile(fileName, blocksize=65536):
    hasher = hashlib.md5()
    afile = open(fileName, "rb")
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()

cpuNum = multiprocessing.cpu_count()    
startTime=time.time()
print hashfile(path + "testFile.jpg", 4096)
endTime=time.time()
executionTime=endTime-startTime
print executionTime/cpuNum

testFile = open(path + "testFile", "r")
while 1:
    print "reading"
    testFile.seek(0)
    print testFile.read()
    sleep(1)