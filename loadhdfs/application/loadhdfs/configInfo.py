#!/usr/bin/env python
#-*- coding:utf-8 -*-
class RefreshConfig(object):
    def __init__(self,interval,hdfspath,filepostfix):
        self.refreshInterval = int(interval)
        self.storePath   = hdfspath
        self.postfix     = filepostfix

    def getInterval(self):
        return self.refreshInterval

    def getStorePath(self):
        return self.storePath
    
    def getPostFix(self):
        return self.postfix