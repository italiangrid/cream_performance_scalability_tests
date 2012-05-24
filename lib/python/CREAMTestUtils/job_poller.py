import os, re, string
import threading
import time
import popen2
import testsuite_utils, job_utils

from submit_pool import JobSubmitterPool

class JobPoller(threading.Thread):

    logger = None

    runningStates = ['IDLE', 'RUNNING', 'REALLY-RUNNING']
    finalStates = ['DONE-OK', 'DONE-FAILED', 'ABORTED', 'CANCELLED']
    
    def __init__(self, parameters, pManager=None, iManager=None):
        threading.Thread.__init__(self)
        self.table = {}
        self.lock = threading.Lock()
        self.parameters = parameters
        self.proxyMan = pManager
        self.interfaceMan = iManager
        
        self.finishedJobs = None
        
        self.jobRE = re.compile("\[?[Jj]ob[Ii][Dd]\]?=\[([^\]]+)")
        self.statusRE = re.compile('\[?[Ss]tatus\]?\s*=\s*\[([^\]]+)')
        self.exitCodeRE = re.compile('\[?[Ee]xitCode\]?\s*=\s*\[([^\]]*)')
        self.failureRE = re.compile('\[?[Ff]ailureReason\]?\s*=\s*\[([^\]]+)')
        self.eventRE = re.compile('EventID=\[([^\]]+)')
        
        self.pool = JobSubmitterPool(parameters, self, pManager)
        self.tableOfResults = {'DONE-OK': 0, 'DONE-FAILED': 0, \
                               'ABORTED': 0, 'CANCELLED': 0}
        
        if JobPoller.logger==None:
            JobPoller.logger = testsuite_utils.mainLogger.get_instance(classid="JobPoller")

    def manageRunningState(self, currId):
        pass
    
    def processFinishedJobs(self):
        if hasattr(self.finishedJobs, 'getPurgeableJobs'):
            jobList = self.finishedJobs.getPurgeableJobs()
        else:
            jobList = self.finishedJobs
        if len(jobList)>0:
            job_utils.eraseJobs(jobList, timeout=self.parameters.sotimeout)
        self.finishedJobs.clear()
    
    def processRunningJobs(self):
        pass
    
    def run(self):

        minTS = time.time()
        
        evStart = 0

        serviceHost = self.parameters.resourceURI[:string.find(self.parameters.resourceURI,'/')]
                
        jobLeft = self.parameters.numberOfJob
        jobProcessed = 0
        isRunning = True
        if self.parameters.queryType=='list':
            listFilename = "/tmp/joblist." + testsuite_utils.applicationID
        else:
            listFilename = None
        
        while (jobProcessed+self.pool.getFailures())<self.parameters.numberOfJob and isRunning:
            
            ts = time.time()
            
            if self.parameters.queryType=='list':
                self.lock.acquire()
                jobList = self.table.keys()
                self.lock.release()
                
                try:
                    tFile = open(listFilename , 'w+b')
                    tFile.write("##CREAMJOBS##\n")
                    for job in jobList:
                        tFile.write(job + "\n")
                    tFile.close()
                except Exception, ex:
                    JobPoller.logger.error(ex)
                    continue
                
                statusCmd = "%s -T %d -i %s" % (testsuite_utils.cmdTable['status'],
                                                self.parameters.sotimeout, listFilename)
                
            elif self.parameters.queryType=='event':
                statusCmd = "%s -d -t %d -e %s %d-%d" % (testsuite_utils.cmdTable['event'],
                                                self.parameters.sotimeout,
                                                serviceHost, evStart, int(time.time())) 
                
            else:
                statusCmd = "%s -f \"%s\" -e %s -T %d --all" % (testsuite_utils.cmdTable['status'], \
                            time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(minTS)), serviceHost, \
                            self.parameters.sotimeout)
                
            JobPoller.logger.debug('Command line: ' + statusCmd)
            
            if self.proxyMan<>None:
                self.proxyMan.beginLock()
            statusProc = popen2.Popen4(statusCmd, True)
                            
            self.lock.acquire()

            try:
                currEvent = -1
                currId = None
                currStatus = None
                currReason = ''
                
                if self.parameters.queryType=='event':
                    triggerOnJobid = False
                else:
                    triggerOnJobid = True

                for line in statusProc.fromchild:
                    if 'ERROR' in line or 'FATAL' in line:
                        JobPoller.logger.error(line[24:])
                        continue
                    
                    if not triggerOnJobid:
                        tmpm = self.eventRE.search(line)
                        if tmpm<>None:
                            jobProcessed += self._updateStatus(currEvent, currId, currStatus, currReason)
                            currEvent = int(tmpm.group(1))
                            currId = None
                            currStatus = None
                            currReason = ''
                            
                            if currEvent>=evStart:
                                evStart = currEvent+1
                            continue
                        
                    tmpm = self.jobRE.search(line)
                    if tmpm<>None:
                        if triggerOnJobid:
                            jobProcessed += self._updateStatus(currEvent, currId, currStatus, currReason)
                            currEvent = 0
                            currStatus = None
                            currReason = ''
                        currId = tmpm.group(1)
                        continue
                        
                    tmpm = self.statusRE.search(line)
                    if tmpm<>None:
                        currStatus = tmpm.group(1).replace('_', '-')
                        continue
                        
                    tmpm = self.failureRE.search(line)
                    if tmpm<>None:
                        currReason = tmpm.group(1)
                        
                jobProcessed += self._updateStatus(currEvent, currId, currStatus, currReason)    
                                
                
            finally:
                statusProc.fromchild.close()
                jobToSend = min(jobLeft, self.parameters.maxRunningJobs - len(self.table))
                self.lock.release()
                if self.proxyMan<>None:
                    self.proxyMan.endLock()

            self.processRunningJobs()
            self.processFinishedJobs()
            
            if jobToSend>0:
                #TODO: imcremental pool feeding
                self.pool.submit(jobToSend)
                snapshot = self.valueSnapshot()
                if len(snapshot)>0:
                    minTS = float(min(snapshot)) -1
                jobLeft = self.parameters.numberOfJob - self.pool.count()
            JobPoller.logger.info("Job left: " + str(jobLeft) + " job processed: " 
                                  + str(jobProcessed+self.pool.getFailures()))
            
            timeToSleep = self.parameters.rate - int(time.time() - ts)
            if timeToSleep>0:
                if self.interfaceMan==None:
                    time.sleep(timeToSleep)
                else:
                    isRunning = self.interfaceMan.sleep(timeToSleep)

        if listFilename<>None:
            try:
                os.remove(listFilename)
            except Exception, e:
                JobPoller.logger.error("Cannot remove %s: %s" % (listFilename, str(e)))


    def put(self, uri, timestamp):
        self.lock.acquire()
        JobPoller.logger.info("Submitted job: " + uri)
        self.table[uri] = timestamp
        self.lock.release()

    def size(self):
        self.lock.acquire()
        result = len(self.table)
        self.lock.release()
        return result

    def valueSnapshot(self):
        self.lock.acquire()
        result = self.table.values()
        self.lock.release()
        return result
    
    def jobIdList(self):
        self.lock.acquire()
        result = self.table.keys()
        self.lock.release()
        return result
            
    def shutdown(self):
        self.pool.shutdown()
        for key in self.tableOfResults:
            JobPoller.logger.info("Job with status %s: %d" % (key, self.tableOfResults[key]))
        return 0
        
    def _updateStatus(self, currEvent, currId, currStatus, currReason):
        if currEvent==-1:
            return 0
            
        if currId==None:
            JobPoller.logger.error('Missing job ID for event ' + str(currEvent))
            return 0
            
        if currStatus==None:
            JobPoller.logger.error('Missing status for ' + currId)
            return 0
            
        if currId in self.table:
            if currStatus in JobPoller.runningStates:
                self.manageRunningState(currId)
            elif currStatus in JobPoller.finalStates:
                del(self.table[currId])
                self.tableOfResults[currStatus] += 1
                self.finishedJobs.append(currId, currStatus, currReason)
                JobPoller.logger.info(
                        "Execution terminated for job: %s (%s, %s)"  
                        % (currId, currStatus, currReason))
                
                if self.parameters.nopurge:
                    self.finishedJobs.dontPurge(currId)
                return 1
            else:
                JobPoller.logger.debug("Status %s for %s" % (currStatus, currId))
                
        return 0
    
