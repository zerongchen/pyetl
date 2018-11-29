#!/usr/bin/env python
# -*- coding: utf-8 -*-
import libxml2
from runConfig import RunConfig

class ConfigXMLParser(RunConfig):
    def __init__(self):
        self.doc              = None
        self.filename         = ""

        
    def parse(self,filename):
        if self.filename != filename:
            self.filename = filename
        if self.doc != None:
            self.doc.freeDoc()
        self.doc      = libxml2.parseFile(self.filename)

    def __del__(self):
        if self.doc is not None:
            self.doc.freeDoc()

    def checkCondition(self):
        if self.doc is None:
            return -1
        return 0

    def getElementValByNode(self,xmlnode,path):
        element = xmlnode.xpathEval(path)
        return element[0].content
    def getElementPropByNode(self,xmlnode,path,property):
        element = xmlnode.xpathEval(path)
        return element[0].prop(property)
        
    def getElementVal(self,path):
        # element list obj
        element = self.doc.xpathEval(path)
        return element[0].content

 



        
        
        
