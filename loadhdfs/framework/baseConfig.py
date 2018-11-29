#!/usr/bin/env python
# _*_ coding: utf-8 _*_
class FileSource(object):
    def __init__(self):
        self.sourcePath = []

    def getSourcePath(self):
        return self.sourcePath

    def addSourcePath(self, sourcePath):
        self.sourcePath.append(sourcePath)

    def toString(self):
        return "{dir:%s}" % (str(self.sourcePath))

class ScannerConfig(object):
    def __init__(self,interval,postFix,filterPostFix="",depth=1):
        self.scannInterval = int(interval)
        self.matchPostFix  = postFix
        self.filterPostFix = filterPostFix
        self.depth         = depth

    def getScanInterval(self):
        return int(self.scannInterval)

    def getFilterPostfix(self):
        return self.filterPostFix

    def getPostFix(self):
        return self.matchPostFix 

    def toString(self):
        return "{scannInterval:%s, matchPostFix:%s, filterPostFix:%s}" % (self.scannInterval,
                                                                          self.matchPostFix,self.filterPostFix)