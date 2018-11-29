#!/usr/bin/env python
# _*_ coding: utf-8 _*_
import time
import sys
import os
from   framework.taskDesc import BaseTaskDesc
from   framework.log import getlogger
class  RefreshMsg(BaseTaskDesc):
    def __init__(self,sender,recver,prefix,filename):
        BaseTaskDesc.__init__(self,sender,recver)
        self.prefix           = prefix
        self.filenameWithPath  = filename
        
    def getPrefix(self):
        return self.prefix

    def getfilename(self):
        return self.filenameWithPath

    def toString(self):
        datastr = "{ prefix:%s, filename:%s }" % (self.prefix,self.filenameWithPath)
        return BaseTaskDesc.toString(self) + datastr