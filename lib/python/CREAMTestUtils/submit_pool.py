import sys, os
import threading
import time
import popen2
import string
import log4py
from testsuite_utils import cmdTable, applicationID, mainLogger

class SubmitterThread(threading.Thread):
    
    logger = None
    
    def __init__(self, pool, scmd, dcmd, lcmd, tName):
        threading.Thread.__init__(self)
        self.setName(tName)
        self.pool = pool
        self.submitFStr = scmd
        self.delegFStr = dcmd
        self.leaseFStr = lcmd
        
        if SubmitterThread.logger==None:
            SubmitterThread.logger = mainLogger.get_instance(classid="SubmitterThread")

    def run(self):
        running = True
        while running:
            running = self.pool.wait()
            if running:
                notified = False
                try:
                    SubmitterThread.logger.debug("Thread submitting: " + self.getName())
                    tmpTS = time.time()
                    
                    if self.delegFStr<>None:
                        delegCmd = self.delegFStr % (os.getpid(), tmpTS)
                        
                        SubmitterThread.logger.debug("Delegate cmd: " +delegCmd)
                        try:
                            if self.pool.proxyMan<>None:
                                self.pool.proxyMan.beginLock()
                                
                            delegProc = popen2.Popen4(delegCmd)
                            for line in delegProc.fromchild:
                                if 'ERROR' in line or 'FATAL' in line:
                                    self.pool.notifySubmitResult(failure=line[24:])
                                    notified = True
                            delegProc.fromchild.close()
                        finally:
                            if self.pool.proxyMan<>None:
                                self.pool.proxyMan.endLock()
                        
                    if not notified and self.leaseFStr<>None:
                        leaseCmd = self.leaseFStr % (os.getpid(), tmpTS)
                        
                        SubmitterThread.logger.debug("Lease cmd: " +leaseCmd)
                        try:
                            if self.pool.proxyMan<>None:
                                self.pool.proxyMan.beginLock()

                            leaseProc = popen2.Popen4(leaseCmd)
                            for line in leaseProc.fromchild:
                                if 'ERROR' in line or 'FATAL' in line:
                                    self.pool.notifySubmitResult(failure=line[24:])
                                    notified = True
                            leaseProc.fromchild.close()
                        finally:
                            if self.pool.proxyMan<>None:
                                self.pool.proxyMan.endLock()
                        
                    if not notified:
                        
                        if self.delegFStr<>None and self.leaseFStr<>None:
                            submCmd = self.submitFStr % (os.getpid(), tmpTS, os.getpid(), tmpTS)
                        elif self.delegFStr<>None or self.leaseFStr<>None:
                            submCmd = self.submitFStr % (os.getpid(), tmpTS)
                        else:
                            submCmd = self.submitFStr
                                            
                        SubmitterThread.logger.debug("Submit  cmd: " + submCmd)
                        tmpErr = 'Internal error:'
                        try:
                            if self.pool.proxyMan<>None:
                                self.pool.proxyMan.beginLock()

                            submitProc = popen2.Popen4(submCmd)
                            for line in submitProc.fromchild:
                                if line[0:8]=='https://':
                                    self.pool.notifySubmitResult(jobId=string.strip(line), timestamp=tmpTS)
                                    notified = True
                                    break
                                if 'ERROR' in line or 'FATAL' in line:
                                    self.pool.notifySubmitResult(failure=line[24:])
                                    notified = True
                                    break
                                tmpErr = tmpErr + string.strip(line) + ";"
                            submitProc.fromchild.close()
                            
                        finally:
                            if not notified:
                                self.pool.notifySubmitResult(failure=tmpErr)
                            if self.pool.proxyMan<>None:
                                self.pool.proxyMan.endLock()

                except:
                    SubmitterThread.logger.error(str(sys.exc_info()[1]))
                    self.pool.notifySubmitResult(failure=str(sys.exc_info()[1]))

class JobSubmitterPool:
    
    logger = None
    
    def __init__(self, parameters, jobTable=None, pManager=None):
        self.jobTable = jobTable
        self.successes = 0
        self.failures = 0
        self.globalSem = threading.Semaphore(1)
        self.masterCond = threading.Condition()
        self.slaveCond = threading.Condition()
        self.running = True
        self.left = 0
        self.processed = 0
        
        if JobSubmitterPool.logger==None:
            JobSubmitterPool.logger = mainLogger.get_instance(classid="JobSubmitterPool")
        
        self.proxyMan = pManager
        
        if parameters.delegationType=='multiple':
            dcmd = '%s -e %s -t %d %s' % (cmdTable['delegate'], \
                                              parameters.resourceURI[:string.find(parameters.resourceURI,'/')],
                                              parameters.sotimeout,
                                              'DELEGID%d.%f')
            delegOpt = '-D DELEGID%d.%f'
        else:
            delegOpt = '-D DELEGID' + applicationID
            dcmd = None
            
        if not hasattr(parameters, 'leaseType') or parameters.leaseType=='none':
            leaseOpt = ''
            lcmd = None
        elif parameters.leaseType=='multiple':
                
            if hasattr(parameters, 'leaseTime'):
                lTime = parameters.leaseTime
            else:
                lTime = 60
                
            lcmd = '%s -e %s -T %d -t %d %s' % (cmdTable['lease'], \
                                      parameters.resourceURI[:string.find(parameters.resourceURI,'/')], \
                                      lTime, parameters.sotimeout, 'LEASEID%d.%f')
            leaseOpt = '-L LEASEID%d.%f'
        else:
            lcmd = None
            leaseOpt = '-L LEASEID' + applicationID
            
        scmd = '%s %s %s -r %s -t %d %s' % (cmdTable['submit'], delegOpt, leaseOpt, \
                                    parameters.resourceURI, parameters.sotimeout, parameters.jdl)
        
        for k in range(0,parameters.maxConcurrentSubmit):
            subThr = SubmitterThread(self, scmd, dcmd, lcmd, "Submitter"  + str(k))
            subThr.start()
        

    def submit(self, numberOfSubmit, resetCounter=False):
        
        if numberOfSubmit<1:
            return 0
        
        self.globalSem.acquire()
        
        self.slaveCond.acquire()
        self.left = numberOfSubmit
        self.processed = 0
        self.slaveCond.release()
        
        go = True
        self.masterCond.acquire()
        if resetCounter:
            self.successes = 0
            self.failures = 0
        
        while go:
            self.slaveCond.acquire()
            if self.left>0:
                self.slaveCond.notifyAll()
            self.slaveCond.release()
            
            go = self.processed<numberOfSubmit
            if go:
                self.masterCond.wait()
            
        self.masterCond.release()
        self.globalSem.release()
        return 0

    def __call__(self, numberOfSubmit):
        self.submit(numberOfSubmit)
            
    def notifySubmitResult(self, jobId='', failure=None, timestamp=0):        
        self.masterCond.acquire()
        if jobId<>'':
            if self.jobTable<>None:
                self.jobTable.put(jobId, timestamp)
            self.successes += 1
        if failure<>None:
            JobSubmitterPool.logger.error(failure)
            self.failures += 1
        self.processed += 1
        self.masterCond.notify()
        self.masterCond.release()

    def wait(self):
        self.slaveCond.acquire()
        while self.left<1 and self.running:
            self.slaveCond.wait()
        self.left -= 1
        result = self.running
        self.slaveCond.release()
        return result
        
    def getSuccesses(self):
        self.masterCond.acquire()
        result =  self.successes
        self.masterCond.release()
        return result

    def getFailures(self):
        self.masterCond.acquire()
        result =  self.failures
        self.masterCond.release()
        return result
    
    def count(self):
        self.masterCond.acquire()
        result = self.successes + self.failures
        self.masterCond.release()
        return result
    
    def shutdown(self):
        self.slaveCond.acquire()
        self.running = False
        self.slaveCond.notifyAll()
        self.slaveCond.release()

class MokeObject:
    def __init__(self, resource, jdl, delegationID=''):
        self.maxConcurrentSubmit = 5
        self.delegationID = delegationID
        self.resourceURI = resource
        self.jdl = jdl
        
    def put(self, uri, timestamp):
        print " -- " + uri

        
def main():
    if len(sys.argv)>4:
        delegationID = sys.argv[4]
    else:
        delegationID = ''
        
    mokeObj = MokeObject(sys.argv[1], sys.argv[2], delegationID)
    pool = JobSubmitterPool(mokeObj, mokeObj)
    pool.submit(int(sys.argv[3]))
    pool.shutdown()

if __name__ == "__main__":
    main()
