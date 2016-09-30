'''
Created on 21 nov. 2015

@author: Roman
'''

import os
from time import time
from time import sleep

from multiprocessing import cpu_count
from multiprocessing import Process

from dircache import listdir

from json.encoder import JSONEncoder
from json.decoder import JSONDecoder

from hashlib import md5
from random import randint

workingFilesPath = '\\\\CAROLINA-PC\\tema2\\working-files\\'

path = '\\\\CAROLINA-PC\\tema2\\working-dir\\'
checkAlivePath = '\\\\CAROLINA-PC\\tema2\\working-dir\\checkAlive-log\\'
todoLogFolder = '\\\\CAROLINA-PC\\tema2\\working-dir\\todo-log\\'
resultLogPath = '\\\\CAROLINA-PC\\tema2\\working-dir\\result-log\\'

# execTime = computePower()
#     print 'Execution time is ', execTime
#searching -> waiting -> computing -> ready -> leader/working
searchingStatus = 'searching'
waitingStatus = 'waiting'
computingStatus = 'computing'
readyStatus = 'ready'
leaderStatus = 'leader'
workingStatus = 'working'

statusKey = 'status'
listOfUnitsKey = 'listOfUnits'
execTimeKey = 'execTime'
unitsToDoTimeKey = 'unitsToDoTime'


def md5hashfile(fileName, blocksize=4096):
    hasher = md5()
    afile = open(fileName, "rb")
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.digest()

def response_alive_task(ID):
    while 1:
        listPath = listdir(checkAlivePath);
        for unit in listPath:
            if 'to'+ID in unit:
                try:
                    if os.path.isfile(checkAlivePath + unit): 
                        os.remove(checkAlivePath + unit)
#                         print "response removed"
                except Exception:
#                         print "response remove error"
                    pass
        sleep(0.1)

def check_alive_task(ID):
    while 1:
        try:
            for unit in listdir(checkAlivePath):
                if ID+'to' in unit:
                    os.remove(checkAlivePath + unit)
                    if os.path.isfile(path + unit.replace(ID+'to', '')):
                        os.remove(path + unit.replace(ID+'to', ''))
            for unit in listdir(path):
                if 'ID' in unit:
                    if ID != unit:
                        tmpFile = open(checkAlivePath + ID + 'to' + unit, 'a')
                        tmpFile.close()
        except Exception:
            pass
        sleep(5)

def leader_task(ID, myUnitData):
    print 'Start Leader task'
    while 1:
        try:
            # extract all units
            pathList = listdir(path)
            todoLogPath = listdir(todoLogFolder)
            unitList = [s for s in pathList if "ID" in s]
            unitList.pop(unitList.index(ID, ))
            doUnitList = [s for s in todoLogPath if "ID" in s and "todo" in s]
            workingUnitList = [s for s in todoLogPath if "ID" in s and "work" in s]
            doneUnitList = [s for s in todoLogPath if "ID" in s and "done" in s]
            resultUnitList = [s for s in resultLogPath if "done" in s]
            # processing work done units
            for unit in doneUnitList:
                for idUnit in unitList:
                    if idUnit in unit:
                        try:
                            os.rename(todoLogFolder+unit, resultLogPath+unit.replace(idUnit+'done', '')+'.done')
                        except Exception:
                            pass
            #get list of unprocessed files
            workingFileList = listdir(workingFilesPath)
            resultFileList = listdir(resultLogPath)
            for workFile in workingFileList:
                for resultFile in resultFileList:
                    if workFile in resultFile:
                        workingFileList.pop(workingFileList.index(workFile))
            #check free units and assign work
            for unit in unitList:
                if not any(unit in s for s in workingUnitList):
                    if not any(unit in s for s in resultUnitList):
                        if not any(unit in s for s in doUnitList):
                            # get a random integer for working file list
                            index = randint(0, len(workingFileList))
                            open(todoLogFolder+unit+'todo'+workingFileList[index], "a+")
                            workingFileList.pop(index)
                        else:
                            if unit in myUnitData[unitsToDoTimeKey]:
                                myUnitData[unitsToDoTimeKey][unit] += 1
                            else:
                                myUnitData[unitsToDoTimeKey][unit] = 0
        except Exception:
            print "Leader error"
        sleep(2)

leaderAliveTimeout = 5
def worker_task(ID, myUnitData):
    global leaderAliveTimeout
    print 'Start Worker task'
    while 1:
        try:
            pathList = listdir(path)
            todoLogPath = listdir(todoLogFolder)
            unitList = [s for s in pathList if "ID" in s]
            unitList.pop(unitList.index(ID, ))
            doUnitList = [s for s in todoLogPath if "ID" in s and "todo" in s]
            for unit in doUnitList:
                if ID in unit:
                    try:
                        os.rename(todoLogFolder+unit, todoLogFolder+unit.replace('todo', 'work'))
                        unit = unit.replace('todo', 'work')
                        result = md5hashfile(workingFilesPath+unit.replace(ID+'work', ''))
                        afile = open(todoLogFolder+unit, 'w')
                        afile.write(result)
                        afile.close()
                        os.rename(todoLogFolder+unit, todoLogFolder+unit.replace('work', 'done'))
                    except Exception:
                        pass
            for unit in unitList:
                # check if unit is leader
                if 'ID' in unit:
                    if os.path.isfile(path+unit) and os.path.getsize(path + unit) > 0:
                        #extract data
                        unitFile = open(path + unit, 'rb')
                        unitData = JSONDecoder().decode(unitFile.read())
                        unitFile.close()
                        if unitData[statusKey] == leaderStatus:
                            leaderAliveTimeout = 5
            leaderAliveTimeout -= 1
            if leaderAliveTimeout == 0:
                myUnitData[statusKey] = searchingStatus
                return -1
        except Exception:
            print "Worker error"
        sleep(2)

def computePower():
    cpuNum = cpu_count()    
    startTime = time()
    md5hashfile(path + "testFile.jpg")
    endTime = time()
    executionTime = endTime - startTime
    return executionTime/cpuNum

def flushMyUnitData(ID, myUnitData):
    try:
        myUnitContent = JSONEncoder(indent=4).encode(myUnitData)
        myUnitfile = open(path + ID, 'w')
        myUnitfile.seek(0)
        myUnitfile.write(myUnitContent)
        myUnitfile.close()
    except Exception:
        pass

def main():
    ID = 'ID' + str(randint(0, 99999))
    myUnitData = {
    'status': 'searching',
    'listOfUnits': [],
    'execTime': 0,
    'unitsToDoTime': {
        'temp': 0
        }
    }
    
    leaderProcess = Process(target=leader_task, args=(ID, myUnitData,))
    workerProcess = Process(target=worker_task, args=(ID, myUnitData,))
    
    checkAliveTask = Process(target=check_alive_task, args=(ID,))
    responseAliveTask = Process(target=response_alive_task, args=(ID,))
            
    searchTime = 5
    
    print 'I am unit with ' + ID
    
    IDfile = open(path + ID, 'a')
    IDfile.close()
    
    flushMyUnitData(ID, myUnitData)
    
    checkAliveTask.start()
    responseAliveTask.start()
    
    while 1:
        IDfile = open(path + ID, 'a')
        IDfile.close()
        
        if myUnitData[statusKey] == searchingStatus:
#             try:
            print ID + ' is ' + searchingStatus
            if searchTime > 0:
                #add all units in path to a list
                unitList = []
                for unit in listdir(path):
                    if 'ID' in unit:
                        unitList.append(unit)
                if len(unitList) > len(myUnitData[listOfUnitsKey]):
                    for unit in unitList:
                        #if is a new unit exist
                        if unit not in myUnitData[listOfUnitsKey]:
                            #add to list
                            myUnitData[listOfUnitsKey].append(unit)
                            #flush to file new unit
                            flushMyUnitData(ID, myUnitData)
                            #set wait timer to 10s waiting
                            searchTime = 3
                elif len(unitList) < len(myUnitData[listOfUnitsKey]):
                    for unit in myUnitData[listOfUnitsKey]:
                        if unit not in unitList:
                            myUnitData[listOfUnitsKey].pop(myUnitData[listOfUnitsKey].index(unit))
                            #flush to file new unit
                            flushMyUnitData(ID, myUnitData)
                            #set wait timer to 10s waiting
                            searchTime = 3
                else:
                    #Decrement timer
                    searchTime -= 1
            else:
                myUnitData[statusKey] = computingStatus
                #flush to file new unit
                flushMyUnitData(ID, myUnitData)
#             except Exception:
#                 print "Error at status: searching"
        elif myUnitData[statusKey] == computingStatus:
            try:
                print ID + ' is ' + computingStatus
                myUnitData[execTimeKey] = computePower()
                myUnitData[statusKey] = readyStatus
                flushMyUnitData(ID, myUnitData)
            except Exception:
                print "Error at status: computing"
        elif myUnitData[statusKey] == readyStatus:
            try:
                print ID + ' is ' + readyStatus
                readyUnits = 0
                #for all units in path
                for unit in myUnitData[listOfUnitsKey]:
                    if 'ID' in unit:
                        if os.path.isfile(path+unit) and os.path.getsize(path + unit) > 0:
                            #extract data
                            try:
                                unitFile = open(path + unit, 'r')
                                unitData = JSONDecoder().decode(unitFile.read())
                                unitFile.close()
                                if unitData[statusKey] == leaderStatus:
                                    myUnitData[statusKey] = workingStatus
                                elif unitData[statusKey] == readyStatus or unitData[statusKey] == workingStatus or unitData[statusKey] == leaderStatus:
                                    readyUnits += 1
                                #flush result
                                flushMyUnitData(ID, myUnitData)
                            except Exception:
                                print "error 342"
                        else:
                            myUnitData[listOfUnitsKey].pop(myUnitData[listOfUnitsKey].index(unit))
                #if all units are ready
                if readyUnits == len(myUnitData[listOfUnitsKey]):
                    #for all units in path
                    for unit in myUnitData[listOfUnitsKey]:
                        if 'ID' in unit:
                            if os.path.isfile(path+unit) and os.path.getsize(path + unit) > 0:
                                #extract data
                                unitFile = open(path + unit, 'r')
                                unitData = JSONDecoder().decode(unitFile.read())
                                unitFile.close()
                                if unitData[execTimeKey] < myUnitData[execTimeKey]:
                                    myUnitData[statusKey] = workingStatus
                    if myUnitData[statusKey] == readyStatus:
                        myUnitData[statusKey] = leaderStatus
                flushMyUnitData(ID, myUnitData)
            except Exception:
                print "Error at status: ready"
        elif myUnitData[statusKey] == leaderStatus:
            print ID + ' is ' + leaderStatus
            if not leaderProcess.is_alive():
                leaderProcess.start()
        elif myUnitData[statusKey] == workingStatus:
            print ID + ' is ' + workingStatus
            if not workerProcess.is_alive():
                if workerProcess.exitcode == None:
                    workerProcess.start()
                else:
                    del workerProcess
                    workerProcess = Process(target=worker_task, args=(ID, myUnitData,))
                    myUnitData[statusKey] = searchingStatus
        sleep(2)
    
if __name__ == "__main__":
    main()
