#!/usr/bin/env python
# _*_ coding: utf-8 _*_
import os
import sys

def getCurPath():
    if os.path.isfile(sys.path[0]):
        return os.path.dirname(sys.path[0])
    return sys.path[0]

def getRootPath():
    srcPath       = getCurPath()
    srcPathFields = srcPath.split(os.sep)
    grandFatherPathFields = srcPathFields[0:-1]
    grandFatherPathFields.pop()
    grandFatherPath = os.sep.join(grandFatherPathFields)
    return grandFatherPath

def getLoadHdfsPath():
    srcPath       = getCurPath()
    srcPathFields = srcPath.split(os.sep)
    grandFatherPathFields = srcPathFields[0:-1]
    grandFatherPath = os.sep.join(grandFatherPathFields)
    return grandFatherPath

def getImportlibPath():
    srcPath       = getCurPath()
    srcPathFields = srcPath.split(os.sep)
    grandFatherPathFields = srcPathFields[0:-1]
    grandFatherPathFields.pop()
    grandFatherPathFields.append("importlib-1.0.3")
    grandFatherPath = os.sep.join(grandFatherPathFields)
    return grandFatherPath

def getFrameworkFile():
    srcPath = getCurPath()
    srcPathFields = srcPath.split(os.sep)
    grandFatherPathFields = srcPathFields[0:-1]
    grandFatherPathFields.append("conf")
    configPath = os.sep.join(grandFatherPathFields)
    configFile = os.path.join(configPath, "framework.ini")
    return configFile

def getLogPath():
    srcPath = getCurPath()
    srcPathFields = srcPath.split(os.sep)
    grandFatherPathFields = srcPathFields[0:-1]
    grandFatherPathFields.append("log")
    logPath = os.sep.join(grandFatherPathFields)
    return logPath

def getConfDirPath():
    srcPath = getCurPath()
    srcPathFields = srcPath.split(os.sep)
    grandFatherPathFields = srcPathFields[0:-1]
    grandFatherPathFields.append("conf")
    confPath = os.sep.join(grandFatherPathFields)
    return confPath

def getDataDirPath():
    srcPath = getCurPath()
    srcPathFields = srcPath.split(os.sep)
    grandFatherPathFields = srcPathFields[0:-1]
    grandFatherPathFields.append("data")
    dataPath = os.sep.join(grandFatherPathFields)
    return dataPath