#!/usr/bin/env python
#_*_ coding: utf-8 _*_
from   utils.xmlConfigParser import ConfigXMLParser
from   framework  import    baseConfig 
class HiveAccConfig(object):
    def __init__(self,path,warehouse):
        self.bin       = path
        self.warehouse = warehouse

    def getWareHouse(self):
        return self.warehouse

    def getHivePath(self):
        return self.bin
        
    def toString(self):
        return "HiveAccConfig(bin:%s,warehouse:%s)\n" % (self.bin,self.warehouse)


class FileType(object):
    def __init__(self,subpath,hiveTab,prefix,partitionKey,bCompress,bRefresh,dataSourceType=None,bPart=False):
        self.subpath      = subpath
        self.hiveTabname  = hiveTab
        self.filePrefix   = prefix
        self.partitionKey = partitionKey
        self.bCompress    = bCompress
        self.bRefresh     = bRefresh
        self.sourceType   = dataSourceType
        self.bPartition   = bPart


    def getHiveTabName(self):
        return self.hiveTabname

    def getPartitionKey(self):
        return self.partitionKey

    def IsCompress(self):
        if cmp(self.bCompress.lower(),"false") == 0:
            return False
        return True 

    def IsNeedRefresh(self):
        if self.bRefresh is None:
            return False
        if cmp(self.bRefresh,"false") == 0:
            return False
        return True

    def IsNeedPart(self):
        if self.bPartition is None:
            return False
        if cmp(self.bPartition,"false") == 0:
            return False
        return True

    def toString(self):
        return "FileType(subpath:%s,hiveTabname:%s filePrefix:%s partitionKey:%s IsNeedPart:%s)\n" % (self.subpath,self.hiveTabname,self.filePrefix,self.partitionKey,self.bPartition)


class FileTypesConfig(object):
    def __init__(self):
        self.fileTypes = {}

    def addFileType(self,prefix,fileTypeObj):
        self.fileTypes[prefix] = fileTypeObj

    def getFileType(self,prefix):
        if self.fileTypes.has_key(prefix):
            return self.fileTypes[prefix]
        return None

    ''''
    def getHiveTabName(self,filenamewithPath):
        for tabname in self.fileTypes.keys():
            if filenamewithPath.find(tabname) != -1:
                return tabname
        return None
'''

    def getKeys(self):
        return self.fileTypes.keys()


    def toString(self):
        outputInfo = "FileTypes:\n"
        for key,value in self.fileTypes.items():
            line = "key:%s value:%s" % (key,value.toString())
            outputInfo += line 
        return outputInfo

class BaseCuConfigParser(ConfigXMLParser):
    def __init__(self):
        ConfigXMLParser.__init__(self)

    #
    # for example
    #
    #   runPath       = "/config/Hive/Path"
    #   warehousePath = "/config/Hive/Warehouse"
    def parseHiveAccConfig(self,runPath,warehousePath):
        if self.checkCondition() != 0:
            return None
        if None in (runPath,warehousePath):
            return None
        try:
            hivePath   = self.getElementVal(runPath)
            warehouse  = self.getElementVal(warehousePath)
            return HiveAccConfig(hivePath,warehouse)
        except:
            print 'parse Hive Config failed for:%s' % (util.getExceptInfo())
            return None

    #
    #   for example 
    #       fileSrcPath       = "/config/FileSource/RootPath"
    #
    def parseFileSource(self,fileSrcPath):
        if self.checkCondition() != 0:
            return None
        if fileSrcPath is None:
            return None

        fileSourceList = baseConfig.FileSource()
        try:
            elements = self.doc.xpathEval(fileSrcPath)
            for element in elements:
                fileSourceList.addSourcePath(element.content)
            return fileSourceList
        except:
            print 'parse FileSource failed for:%s' % (util.getExceptInfo())
            return None

    #   for example
    #        intervalPath = "/config/Scanner/Interval"
    #        postfixPath  = "/config/Scanner/PostFix"
    #
    def parseScannerConfig(self,intervalPath,postfixPath):
        if self.checkCondition() != 0:
            return None
        if None in (intervalPath,postfixPath):
            return None
        try:
            scannerInterval       = self.getElementVal(intervalPath)
            postFix               = self.getElementVal(postfixPath)
            return baseConfig.ScannerConfig(scannerInterval,postFix)
        except:
            print 'parse Scanner Config failed for:%s' % (util.getExceptInfo())
            return None

    #
    #  for example
    #       fileTypesPath  = "/config/FileTypes/FileInfo"
    #       prefixKey  = "prefix"
    #       subpathKey = "subpath"
    #       hiveTabKey = "hiveTab"
    #       partionKey = "partitionBy"
    #       compressKey = "compress"
    #       refreshKey  = "refresh"
    #       datypeKey   = "type"
    #       partition   = "ispart"
    def parseFileTypes(self,fileTypesPath,prefixKey,subpathKey,hiveTabKey,partionKey,compressKey,refreshKey,datypeKey,ispartKey):
        if self.checkCondition() != 0:
            return None
        if fileTypesPath  is None:
            return None
        fileTypes = FileTypesConfig()
        try:
            elements = self.doc.xpathEval(fileTypesPath)
            for element in elements:
                prefix = (prefixKey == None) and None or element.prop(prefixKey)
                subpath = (subpathKey == None) and None or element.prop(subpathKey)
                tblname = (hiveTabKey == None) and None or element.prop(hiveTabKey)
                partitionBy = (partionKey == None) and None or element.prop(partionKey)
                compress  = (compressKey == None) and None or element.prop(compressKey)
                refresh   = (refreshKey == None) and None or element.prop(refreshKey)
                sourceType = (datypeKey == None) and None or element.prop(datypeKey)
                ispart = (ispartKey == None) and None or element.prop(ispartKey)
                fileType = FileType(subpath,tblname,prefix,partitionBy,compress,refresh,sourceType,ispart)
                fileTypes.addFileType(prefix,fileType)
            return fileTypes
        except:
            print 'parse FileTypes failed for:%s' % (util.getExceptInfo())
            return None



