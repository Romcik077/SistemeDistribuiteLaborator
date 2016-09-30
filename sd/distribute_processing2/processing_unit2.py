'''
Created on 21 nov. 2015

@author: Roman
'''

import os

from time import sleep

from dircache import listdir

from hashlib import md5
from random import randint

workingFilesPath = '\\\\CAROLINA-PC\\tema3\\working-files\\'
resultPath = '\\\\CAROLINA-PC\\tema3\\result-log\\'

def md5hashfile(fileName, blocksize=4096):
    hasher = md5()
    afile = open(fileName, "rb")
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.digest()

def main():
    ID = 'ID' + str(randint(0, 99999))
    while 1:
        try:
            print "I am unit with " + ID
            workingFileList = listdir(workingFilesPath)
            readyFileList = [s for s in workingFileList if ".ready" in s]
            busyFileList = [s for s in workingFileList if ".busy" in s]
            print "Ready list have length" + str(len(readyFileList))
            if len(readyFileList) > 0:
                index = randint(0, len(readyFileList))
                os.rename(workingFilesPath+readyFileList[index], workingFilesPath+readyFileList[index].replace(".ready", "."+ID+".busy"))
                readyFileList[index] = readyFileList[index].replace(".ready", "."+ID+".busy")
                if os.path.isfile(workingFilesPath + readyFileList[index]):
                    print readyFileList[index]
                    result = md5hashfile(workingFilesPath + readyFileList[index])
                    resultFile = open(resultPath+readyFileList[index], 'a')
                    resultFile.write(result)
                    resultFile.close()
                    os.rename(workingFilesPath+readyFileList[index], workingFilesPath+readyFileList[index].replace("."+ID+".busy", "."+ID+".done"))
                    os.rename(resultPath+readyFileList[index], resultPath+readyFileList[index].replace("."+ID+".busy", "."+ID+".done"))
            elif len(busyFileList) > 0:
                index = randint(0, len(busyFileList))
                os.rename(workingFilesPath+readyFileList[index], workingFilesPath+readyFileList[index].replace(".busy", "."+ID+".busy"))
                readyFileList[index] = readyFileList[index].replace(".busy", "."+ID+".busy")
                if os.path.isfile(readyFileList[index]):
                    result = md5hashfile(readyFileList[index])
                    resultFile = open(resultPath+readyFileList[index], 'a')
                    resultFile.write(result)
                    resultFile.close()
                    os.rename(workingFilesPath+readyFileList[index], workingFilesPath+readyFileList[index].replace("."+ID+".busy", "."+ID+".done"))
                    os.rename(resultPath+readyFileList[index], resultFile+readyFileList[index].replace("."+ID+".busy", "."+ID+".done"))
        except Exception:
            print "Error, try next time"
        sleep(2)

if __name__ == "__main__":
    main()
    