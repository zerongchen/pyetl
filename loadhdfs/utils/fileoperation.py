#!/usr/bin/env python
# -*- coding: utf-8 -*-
import traceback
import sys
import os
import time,datetime
import shutil
import glob
from   framework.log import getlogger
from utils import util

def compressFilePatternCheck(filename):
    #getlogger().info("processName:%s check compress file: %s",self.name,filename)
    postfix = filename.split(".")[-1]
    if postfix =="gz":
        return True
    return False

def compressFileByTar(filename,compressedFilename):
    if not isFileExist(filename):
        getlogger().warning("compress file:%s no exist",filename)
        return False
    cmdStr        = "tar zcfP %s %s" %(compressedFilename,filename)
    retVal,output = util.executeCMD3(cmdStr)
    if retVal != 0 and retVal !=2:
        getlogger().warning("compress file:%s failed err:%d",filename,retVal)
        return False
    return True

def uncompressFile(filename,destDir):
    if not isFileExist(filename):
        getlogger().warning("compress file:%s no exist",filename)
        return False
    if not compressFilePatternCheck(filename):
        getlogger().warning("compress file:%s check fail",filename)
        return False
    cmdStr = "tar zxfP " + filename + " -C "+destDir
    retVal,output = util.executeCMD3(cmdStr)
    if retVal != 0 and retVal !=2:
        getlogger().warning("compress file:%s failed err:%d",filename,retVal)
        return False
    return True

# just add *.lzo to the tail of filename 
def compresessFile(filename,destDir, logger):
    changDir = ""
    if destDir != os.getcwd():
        changDir = " -p" + destDir
    cmdStr = "lzop -1 -f -U  " + filename + changDir
    retVal ,output = util.executeCMD3(cmdStr)
    if retVal != 0:
        logger.error("[%s] exe:'%s' failed result:%s", os.path.basename(__file__), cmdStr, str(retVal))
        return False
    return True
          

def isFileExist(dirInfo):
    return os.path.exists(dirInfo)

def renameFile(srcFilename,destFilename):
    try:
        os.rename(srcFilename,destFilename)
        return True
    except:
        getlogger().warning("rename file failed for:%s",util.getExceptInfo())
        return False

def moveFile(srcFilename,destFilename):
    files = glob.glob(srcFilename)
    for singleFile in files:
        shutil.move(singleFile,destFilename)
    return True

def removeFile(filename):
    try:
        if not isFileExist(filename):
            return True
        os.remove(filename)
        return True
    except:
        getlogger().warning("remove file %s failed for:%s",filename,util.getExceptInfo())
        return False

def fileIsEmpty(filename):
    size = 0L
    size = os.path.getsize(filename)
    if size == 0:
	getlogger().info("file:%s size is 0",filename)
        return True
    return False

def createDir(dirInfo):
    try:
        os.makedirs(dirInfo)
        return True
    except:
	getlogger().warning("mkdir %s fail",dirInfo)
	return False

def createFile(filename):
    bRet = False
    fobj = None
    try:
        fobj = open(filename,"w")
        bRet = True
    except:
        getlogger().warning("createFile:%s failed for:%s",filename,util.getExceptInfo())
        bRet = False
    finally:
        if fobj != None: fobj.close()
        return bRet

	
def checkOrCreateDir(dirInfo):
    if not isFileExist(dirInfo):
        return createDir(dirInfo)
    return True

def checkOrCreateFile(filename):
    if not isFileExist(filename):
        return createFile(filename)
    return True

#filename --- filename with path
def getFirstline(filename):
    dataline = None
    if not os.path.exists(filename):
        getlogger().warning("filename :%s no exist",filename)
        return None
    try:
        fileHandle = open(filename,"r")
	for line in fileHandle:
            dataline = line
	    break
        fileHandle.close()
        return dataline
    except:
	getlogger().warning("open and read file:%s failed for:%s",filename,util.getExceptInfo())
        return None

def fileFormatCheck(filename,delimeter,fieldSum):
    if not os.path.exists(filename):
        getlogger().warning("filename :%s no exist",filename)
        return  False
    try:
        fileHandle = open(filename,"r")
        for line in fileHandle:
            fields = line.split(delimeter)
            if len(fields) != fieldSum:
	        getlogger().warning("line:%s check fail",line)
                return False
        fileHandle.close()
        return True

    except:
	getlogger().warning("open and read file:%s failed for:%s",filename,util.getExceptInfo())
        return False

def fileSize(filename):
    if not os.path.exists(filename):
        getlogger().warning("filename :%s no exist",filename)
        return  0
    return os.path.getsize(filename)

def getFilelines(filename):
    lines = None
    if not os.path.exists(filename):
        getlogger().warning("filename :%s no exist",filename)
        return  lines
    try:
        fp = open(filename,"r")
        lines =  fp.readlines()
    except:
        getlogger().warning("open filename:%s read failed for:%s",filename,util.getExceptInfo())
    finally:
        if fp != None:fp.close()
        return lines




def updateFileLines(filename,newLines):
    bRet = False
    if not os.path.exists(filename):
        getlogger().warning("filename :%s no exist",filename)
        return  bRet
    try:
        fp = open(filename,"r+")
        fp.seek(0)
        fp.truncate(0)
        fp.writelines(newLines)
        bRet = True
    except:
        getlogger().warning("open filename:%s read failed for:%s",filename,util.getExceptInfo())
    finally:
        if fp != None:fp.close()
        return bRet

def openAndWrietData(filename,datalines):
    bRet = False
    try:
        fp = None
        if isFileExist(filename):
            getlogger().warning("filename :%s already exists",filename)
            fp = open(filename,"a")
        else:
            fp  = open(filename,"w+")           
        fp.writelines(datalines)
        bRet = True
    except:
        getlogger().warning("open filename:%s write failed for:%s",filename,util.getExceptInfo())
    finally:
        if fp != None:fp.close()
        return bRet
    
    

            
            
            
    





            

    
       
        
