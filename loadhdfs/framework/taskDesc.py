#!/usr/bin/env python
#-*- coding:utf-8 -*-
class BaseTaskDesc(object):
    def __init__(self,snder,rcver):
        self.snder = snder
        self.rcver = rcver

    def getSender(self):
        return self.snder

    def getRecver(self):
        return self.rcver

    def toString(self):
        return "sender:%s recver:%s\n" % (self.snder,self.rcver)
