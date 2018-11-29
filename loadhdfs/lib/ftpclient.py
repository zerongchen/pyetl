#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import socket
import time
import ftplib
from utils import util
from utils import fileoperation
from framework.log import getlogger
from ftplib import FTP

class FtpClient:
    def __init__(self):
        self.ftp           = FTP()
        self.log           = getlogger()
        self.connected     = False
        self.logined       = False
        self.strIp         = ""
        self.strPort       = ""
        self.username      = ""
        self.password      = ""
        self.retryInterval = 3
        self.cwd           = ""
        self.timeout       = 30
        #self.ftp.set_debuglevel(2)


    def connect(self,strIp,strPort):
        if self.connected:
            return True
        try:
            socket.setdefaulttimeout(self.timeout)
            self.ftp.connect(strIp,strPort) 
            self.log.info('connect to %s:%s OK',strIp,strPort)
            self.connected = True
            return True
        except:
            self.connected = False
            self.logined   = False
            self.log.error('connect to %s:%s failed for:%s',strIp,strPort,util.getExceptInfo())
            return False



    def login(self,username,password):
        if self.logined:
            return True

        if not self.connected:
            self.log.warning("ftp conn have not created yet")
            return False
        try: 
            self.ftp.login(username,password)
            self.log.info('user:%s password:%s ok',username,password)
            self.logined = True
            self.cwd     = ""
            return True
        except:
            self.log.error('user:%s password:%s failed for %s',username,password,util.getExceptInfo())
            return False

    def checkStatus(self):
        if self.logined:
            return True
        return False

    def changedir(self,dataDir):
        if not self.checkStatus():
            self.log.error("ftp client have not logined yet")
            return False

        if self.cwd == dataDir:
            return True
            
        try:
            #print self.ftp.getwelcome()
            self.ftp.cwd(dataDir)
            self.log.info('change dir:%s ok' ,dataDir)
            self.cwd = dataDir
            return True
        except:
            self.log.error('change dir:%s failed for:%s' ,dataDir,util.getExceptInfo())
            return False




    # called when you want to resume the ftp object
    def reinit(self,retries):
        return self.init(self.strIp,self.strPort,self.username,self.password,retries)



    # called after constructor,init will not change remote dir which is a application action
    # this verion don't check the arguments's validation
    def init(self,ip,port,username,password,retries):
        for retry in xrange(retries):
            if not self.connect(ip,port):
                time.sleep(self.retryInterval)
            else:
                break

        if not self.connected:
            self.log.warning('connect %s:%s retries:%d' ,ip,port,retries)
            return False
        self.strIp    = ip
        self.strPort  = port
        self.username = username
        self.password = password
        if not self.logined:
            return self.login(username,password)
            #self.log.log(logging.WARNING,"processName:%s ftp changedir(%s) failed give up current task",self.runCtx.name,self.taskDesc.remotedir)
        else:
            return True



    def rename(self, fromname, toname):
        if not self.checkStatus():
            self.log.warning('Ftp client have not logined yet')
            return False
        try:
            self.ftp.rename(fromname, toname)
            return True
        except:
            self.log.error('ftp rename from %s to %s failed for:%s' ,fromname,toname,util.getExceptInfo())
            return False


    def checkDownloadFile(self,localfile,remotefile):    
        remotefileSize = self.getSize(remotefile)
        localfileSize  = fileoperation.fileSize(localfile)
        if remotefileSize == localfileSize:
            return True
        else:
            self.log.warning("ftp remotefile:%s(size:%d) localfile:%s(size:%d)",remotefile,remotefileSize,localfile,localfileSize)
            return False

    #need to check filename is fullpath 
    #Ret val have diff meaning
    #  0 ---- ok
    #  1 ---- net error
    #  2 ---- io error
    #  3 ---- other error
    def download(self, localfile, remotefile):
        self.log.info("ftp try to download:%s",remotefile)
        iRet = 0
        if not self.checkStatus():
            iRet = 1
            self.log.error("ftp client have not logined yet")
            return iRet
        
        file_handler = None
        try:
            file_handler = open(localfile, 'wb')
            self.ftp.retrbinary('RETR %s'%(remotefile), file_handler.write)
            file_handler.close()
            self.log.info('ftp download:%s success',remotefile)
            return iRet
            #self.ftp.set_debuglevel(0)
        except socket.error:
            self.log.error("ftp download:%s error %s",remotefile,util.getExceptInfo())
            iRet = 1 
        except IOError:
            self.log.error("source file:%s load error %s",remotefile,util.getExceptInfo())
            iRet = 2
        except ftplib.error_perm:
            self.log.error("ftp download:%s for permanate error",remotefile)
            iRet = 3
        except ftplib.error_temp:
            self.log.error("ftp download:%s for temporary error",remotefile)
            iRet = 4
        except:
            self.log.error("ftp download:%s for error:%s",remotefile,util.getExceptInfo())
            iRet = 5

        finally:
            if file_handler != None: file_handler.close()
            return iRet

    def download_ex(self,localfile,remotefile):
        iRet = self.download(localfile,remotefile)
        if iRet == 0:
            if not self.checkDownloadFile(localfile,remotefile):
                fileoperation.removeFile(localfile)
                return False
            return True
        else:
            fileoperation.removeFile(localfile)
            if iRet == 4:
                return True
            else:
                return False

    #need to check filename is fullpath 
    #Ret val have diff meaning
    #  0 ---- ok
    #  1 ---- net error
    #  2 ---- io error
    #  3 ---- ftp permanet error
    #  4 ---- ftp temporay error
    #  5 ---- unknown error
    def upload(self,filename,interPostfix=None):
        bufsize = 1024
        iRet = 0
        fp = None
        if not self.checkStatus():
            iRet = 1
            self.log.error("ftp client have not logined yet")
            return iRet

        try:
            self.ftp.set_pasv(False)
            fp = open(filename,"rb")
            srcFileName = os.path.basename(filename)
            #upFileName = "%s.%s" % (srcFileName,interPostfix)
            #self.ftp.storbinary('STOR %s' % upFileName,fp,bufsize)
            self.ftp.storbinary('STOR %s' % (srcFileName),fp,bufsize)
            #self.ftp.rename(upFileName,srcFileName)
            self.log.info('ftp upload:%s success',filename)
            return iRet
            #self.ftp.set_debuglevel(0)
        except socket.error:
            self.log.error("ftp upload:%s error %s",filename,util.getExceptInfo())
            iRet = 1 
        except IOError:
            self.log.error("source file:%s load error %s",filename,util.getExceptInfo())
            iRet = 2
        except ftplib.error_perm:
            self.log.error("ftp upload:%s for permanate error %s ",filename,util.getExceptInfo())
            iRet = 3
        except ftplib.error_temp:
            self.log.error("ftp upload:%s for temporary error %s",filename,util.getExceptInfo())
            iRet = 4
        except:
            self.log.error("ftp upload:%s for other error %s",filename,util.getExceptInfo())
            iRet = 5
        finally:
            if fp != None: fp.close()
            return iRet

    # in the future version the retval meanings may change
    # retval format operRes,needRetryOrNot
    # False,True    ------  need retry
    # True,*        ------  not need retry
    def upload_ex(self,filename):
        iRet = self.upload(filename)
        if iRet == 0:
            return True,False
        elif iRet == 1:
            self.quit()
            return False,True
        elif iRet == 4:
            return False,True
        else:
            return False,False

  
    def getfiles(self,dir=None):
        files = []
        if not self.checkStatus():
            self.log.error("ftp client have not logined yet")
            return files
        try:
            #self.changedir(dir)     
            self.log.info("current path:%s",self.ftp.pwd())
            files = files + self.ftp.nlst()
        except:
            self.log.error("Get remote files in the directory:%s failed for:%s",dir,util.getExceptInfo())
        return files


    def getSize(self,filename):
        if not self.checkStatus():
            self.log.error("ftp client have not logined yet")
            return -1
        try:
            sizeInfo = self.ftp.size(filename)
            return sizeInfo
        except:
            self.log.error("Get remote files(%s) size failed for:%s",filename,util.getExceptInfo())
            return -1
        
    def getlist(self,dir):
        lists = None
        if not self.checkStatus():
            self.log.error("ftp client have not logined yet")
            return lists
        try:
            self.ftp.cwd(dir)
            files = self.ftp.dir(dir)
        except:
            self.log.error("Get remote files and folders in the directory:%s failed for %s",dir,util.getExceptInfo())
        return lists

    def deletefile(self,filepath):
        if not self.checkStatus():
            self.log.error("ftp client have not logined yet")
            return False
        try:
            self.ftp.delete(filepath)
            return True
        except:
            self.log.error("Delete file:%s failed for :%s",filepath,util.getExceptInfo())
            return False
        
    def deletedir(self, dirpath):
        if not self.checkStatus():
            self.log.error("ftp client have not logined yet")
            return False
        try:
            self.ftp.rmd(dirpath)
            return True
        except:
            self.log.error("Delete folder:%s failed for :%s",dirpath,util.getExceptInfo())
            return False
                
    def quit(self):
        iRet = 0
        if not self.checkStatus():
            iRet = 1
            self.log.error("ftp client have not logined yet")
            return iRet
        self.log.debug("ftp quit")
        try:
            self.ftp.quit()
        except ftplib.all_errors:
            iRet = 1
            self.log.error("ftp related error %s",util.getExceptInfo())
        except socket.error:
            iRet = 2
            self.log.error("net error %s",util.getExceptInfo())
        except IOError:
            iRet = 3
            self.log.error("IO error %s",util.getExceptInfo())
        finally:
            self.connected = False
            self.logined   = False
            return iRet
            
            
        

#if __name__=="__main__":
'''
    ftpCli = FtpClient()
    ftpCli.connect("121.32.136.197","21")
    ftpCli.login('zhengs','123456')
    #ftpCli.changedir('result')
    files = ftpCli.getfiles('result')
    print files
    for item in files:
        ftpCli.download("C:\\Users\\Administrator\\Desktop\\python\\run"+item,item)
    ftpCli.quit()
'''
    
    
            
            

    
       
        
