
import threading
import time
import popen2

import job_utils, testsuite_utils
from submit_pool import JobSubmitterPool

class JobMonitor(threading.Thread):
    logger = None
    
    runningStates = ['IDLE', 'RUNNING', 'REALLY-RUNNING']
    finalStates = ['DONE-OK', 'DONE-FAILED', 'ABORTED', 'CANCELLED']
    
    def __init__(self, parameters, pManager=None, iManager=None):
        threading.Thread.__init__(self)
        self.table = {}
        self.notified = []
        self.lock = threading.Lock()
        self.parameters = parameters
        self.pool = JobSubmitterPool(parameters, self, pManager)
        self.tableOfResults = {'DONE-OK': 0, 'DONE-FAILED': 0, 'ABORTED': 0, 'CANCELLED': 0}
        self.interfaceMan = iManager
        
        self.lastNotifyTS = time.time()
        
        if JobMonitor.logger==None:
            JobMonitor.logger = testsuite_utils.mainLogger.get_instance(classid="JobMonitor")
        
    def manageNotifications(self):
        pass
    
    def processNotifiedJobs(self):
        pass

    def processFinishedJobs(self):
        if hasattr(self.finishedJobs, 'getPurgeableJobs'):
            jobList = self.finishedJobs.getPurgeableJobs()
        else:
            jobList = self.finishedJobs
        if len(jobList)>0:
            job_utils.eraseJobs(jobList, timeout=self.parameters.sotimeout)
        self.finishedJobs.clear()

    def run(self):
        minTS = time.time()
         
        jobLeft = self.parameters.numberOfJob
        jobProcessed = 0
        isRunning =True
        
        while (jobProcessed+self.pool.getFailures())<self.parameters.numberOfJob and isRunning:
            
            ts = time.time()
            
            self.lock.acquire()
            try:
                if (ts - self.lastNotifyTS) > self.parameters.rate*2:
                    JobMonitor.logger.warn("Missed last notify")
                    
                self.manageNotifications()
                    
                for (job, status, fReason) in self.notified:
                    if job in self.table:
                        del(self.table[job])
                        self.finishedJobs.append(job, status, fReason)
                        jobProcessed += 1
                        JobMonitor.logger.info("Terminated job %s with status (%s, %s)" \
                                                        % (job, status, fReason))
                        self.tableOfResults[status] += 1
                        
                        if self.parameters.nopurge:
                            self.finishedJobs.dontPurge(currId) 
                            
                self.notified = []
                        
            except:
                JobMonitor.logger.error(sys.exc_info()[2])

            jobToSend = min(jobLeft, self.parameters.maxRunningJobs - len(self.table))                
            self.lock.release()
            
            self.processNotifiedJobs()
                
            self.processFinishedJobs()
                
            #TODO: imcremental pool feeding
            self.pool.submit(jobToSend)
#            snapshot = self.valueSnapshot()
#            if len(snapshot)>0:
#                minTS = float(min(snapshot)) -1
            jobLeft = self.parameters.numberOfJob - self.pool.count()
            JobMonitor.logger.info("Job left: " + str(jobLeft) + " job processed: " 
                                    + str(jobProcessed+self.pool.getFailures()))
            
            timeToSleep = self.parameters.rate - int(time.time() - ts)
            if timeToSleep>0:
                if self.interfaceMan==None:
                    time.sleep(timeToSleep)
                else:
                    isRunning = self.interfaceMan.sleep(timeToSleep)
                
    def notify(self, jobHistory):
        self.lock.acquire()
        self.lastNotifyTS = time.time()
        (jobId, status, fReason) = jobHistory[-1]
        JobMonitor.logger.debug("Notify %s (%s, %s)" % (jobId, status, fReason))
        if status in JobMonitor.finalStates:
            self.notified.append((jobId, status, fReason))
        self.lock.release()
    
    def put(self, uri, timestamp):
        self.lock.acquire()
        JobMonitor.logger.info("Submitted job: " + uri)
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
            JobMonitor.logger.info("Job with status %s: %d" % (key, self.tableOfResults[key]))
        return 0
