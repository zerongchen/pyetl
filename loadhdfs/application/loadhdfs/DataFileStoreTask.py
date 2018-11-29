#!/usr/bin/env python
#-*- coding:utf-8 -*-
import time
import sys
import os
from   utils import util,hdfsOperation,fileoperation
from   framework.taskDesc import BaseTaskDesc
from   framework.log import getlogger

class DataFileStoreTask(BaseTaskDesc):
    def __init__(self,sender,recver,filenameWithPath):
        BaseTaskDesc.__init__(self,sender,recver)
        self.filename         = filenameWithPath
        self.basename         = os.path.basename(self.filename)
        self.listSplitByUnderline = self.basename.split('_')

    def getfilename(self):
        return self.filename

    def getBasename(self):
        return self.basename

    # city-code
    def getFileArea(self):
        return self.listSplitByUnderline[0]

    # file-identify
    def getFilePrefix(self):
        return self.listSplitByUnderline[1]

    # statis-cycle-time
    def getReportTimeByFilename(self):
        return self.listSplitByUnderline[2]

    def checkFilenameValidation(self):
        listSplitByDot = self.basename.split('.')
        if len(listSplitByDot) >2:
            return False     
        return True
        
    def toString(self):
        resultString = ""
        if self.filename != None:
            resultString += self.filename
        return "{sender:%s, recver:%s, filename:%s}" % (self.snder,self.rcver,resultString)






            

	
	

  

