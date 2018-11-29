#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
from   utils import util
from   utils.cuConfigInfo import BaseCuConfigParser
import configInfo
      
class DataConfigParser(BaseCuConfigParser):
    def __init__(self):
        BaseCuConfigParser.__init__(self)

    #
    #  for example
    #      cyclepath=/config/Refresh/cycle
    #      storepath=/config/Refresh/hdfspath
    #      filepostfix=/config/Refresh/postifx
    #       
    def parserRefresh(self,cyclepath,storepath,filepostfix):
        try:
            refreshInterval  = self.getElementVal(cyclepath)
            hdfspath         = self.getElementVal(storepath)
            postfix          = self.getElementVal(filepostfix)
            return configInfo.RefreshConfig(refreshInterval,hdfspath,postfix)
        except:
            print 'parse Refresh Config failed for:%s' % (util.getExceptInfo())
            return None


#if __name__=="__main__":
#    try:
#        DataConfigParser.instance().parse(sys.argv[1])
#        scannerConfig = DataConfigParser.instance().parseScannerConfig()
#        fileSource    = DataConfigParser.instance().parseFileSource()
#        hiveConfig    = DataConfigParser.instance().parseHiveAccConfig()
#        print scannerConfig.toString()
#        print fileSource.toString()
#        print hiveConfig.toString()
#    except:
#        print 'main failed for :%s' % (util.getExceptInfo())
#    resp = Response('<?xml version="1.0" encoding="utf-8" ?><pcgreenetres length="0149"><result>0</result><remindcount  total="8" /></pcgreenetres>')
#    resp.getMsgNum()