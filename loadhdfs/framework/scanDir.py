#!/usr/bin/env python
# _*_ coding: utf-8 _*_
import os
#from utils import util
import sys
#from   framework.log         import  getlogger
###############################
#
# choose need to upload files #
#
###############################
class Scanner(object):
    def __init__(self,log):
        #self.log           = log
        self.filterPostfix = None
        self.matchPostfix  = None

    def setFilterPostfix(self,postFix):
        self.filterPostfix = ' '.join(postFix.split(','))

    def setMatchPostfix(self,postFix):
        self.matchPostfix = ' '.join(postFix.split(','))

        
    def postFixMatcher(self,filename):
        bRet = filename.split('.')[-1] in self.matchPostfix
        return bRet

    def postFixFilter(self,filename):
        bRet = not filename.split('.')[-1] in self.filterPostfix
        return bRet

    def getfiles(self,path):
        try:
            itemlist = os.listdir(path)

            checkfile = lambda x:not os.path.isdir(os.path.join(path,x))
            files = filter(checkfile,itemlist)

            # do second filter
            restFiles = None
            if self.matchPostfix == None:
                restFiles = filter(self.postFixFilter,files)
            else:
                restFiles = filter(self.postFixMatcher,files)
            return restFiles
        except:
            self.log.error('[%s] get file except %s', __file__, util.getExceptInfo())
            return None
