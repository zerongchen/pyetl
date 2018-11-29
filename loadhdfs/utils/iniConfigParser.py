#!/usr/bin/env python
#_*_ coding: utf-8 _*_
from ConfigParser import ConfigParser
from runConfig import RunConfig
class IniConfigParser(RunConfig):
    def __init__(self):
        self.config = ConfigParser()
        
    def parse(self,filename): 
        self.config.read(filename)
        return True
    
    def getConfVal(self,sectname,itemname):
        return self.config.get(sectname,itemname)


#if __name__=="__main__":
#    configObj = RunConfig.instance()
#    configObj.readConfig("../conf/config")
#    print configObj.getConfVal("FTPSERVER","host")
#    print configObj.getConfVal("FTPSERVER","port")
#    print configObj.getConfVal("FTPSERVER","user")
#    print configObj.getConfVal("FTPSERVER","password")
#    print configObj.getConfVal("FTPSERVER","uppath")
