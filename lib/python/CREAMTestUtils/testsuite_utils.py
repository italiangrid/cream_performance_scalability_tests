

import sys, os, os.path, tempfile
import re, string
import time
import popen2
import getopt
import log4py
import tty, termios, threading

class BadValueException(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return str(self.message)
    
def checkIsOk(value):
    return True

def checkRate(value):
    if value<5:
        raise BadValueException('Bad rate ' + repr(value))

def atLeastOne(value):
    if value<1:
        raise BadValueException('Bad number for job ' + repr(value))
    
def checkPort(value):
    if value<1025 or value>65535:
        raise BadValueException("Bad port number: " + repr(value))
    
def checkResourceURI(value):
    if value=='':
        raise BadValueException("Missing resource parameter")
    
def checkVO(value):
    if value=='' and getUserKeyAndCert()[0]<>None:
        raise BadValueException("Parameter vo is mandatory using personal credentials")
    
def checkValid(value):
    tokens = string.split(value, ':')
    if len(tokens)<>2:
        raise BadValueException("Bad valid value: "  + value)
    
    h = int(tokens[0])
    m = int(tokens[1])
    if h<0 or h>23 or m<0 or m>59:
        raise BadValueException("Bad valid value: " + value)
    
def checkRenewType(value):
    if value<>'none' and value<>'single' and value<>'multiple':
        raise BadValueException("Bad type: " + value)
    
def checkFile(value):
    if value<>'' and not os.path.isfile(value):
        raise BadValueException, "Bad file: " + value
    
def checkQueryType(value):
    if value<>'event' and value<>'list' and value<>'timestamp':
        raise BadValueException, "Bad query type: " + value
    
def checkEncryptedKey(file):
    if not os.path.exists(file):
        raise Exception, "Missing key file"
    
    try:
        keyFile = open(file)
        for line in keyFile:
            if 'ENCRYPTED' in line:
                return True
    finally:
        keyFile.close()
    return False
                
    
class Parameters:
    def __init__(self, shortDescr, synopsis, description):
        print "init di testsuite_utils.Parameters"
        self.pTable = {}
        self._shortDescr = shortDescr
        self._synopsis = synopsis
        self._description = description
        self._env = []
        
        self.register('help', 'b', descr='Print the man pages for this command')
        self.register('logConf', 's', check=checkFile, descr='''\
Set the location of the configuration file for log4py, \
(DEFAULT as provided by log4py)''')
        self.register('interactive','b', optChar='', 
                      descr='Enable the test control via terminal (EXPERIMENTAL)')
        self.register('nopurge','b', optChar='',
                      descr='Disable the purge operation for all jobs submitted (CRITICAL)')
        self.register('sotimeout', 'd', 30, optChar='',
                      descr='Socket timeout in seconds (DEFAULT 30s)')
        
        
    def addEnvItem(self, item):
        self._env.append(item)
        
    def setDefaultEnvs(self, consumerKeys=False):
        self.addEnvItem(('GLITE_LOCATION', 'location of gLite packages (DEFAULT=/opt/glite)'))
        self.addEnvItem(('X509_USER_PROXY', 'location of the user proxy'))
        self.addEnvItem(('X509_USER_CERT', 'location of the user certificate, this variable has \
priority upon the X509_USER_PROXY variable'))
        self.addEnvItem(('X509_USER_KEY', '''location of the user private key. \
The key must be protected by a passphrase.'''))
        
        if consumerKeys:
            self.addEnvItem(('X509_CONSUMER_CERT','''location of the consumer certificate, \
if this variable is set together with X509_CONSUMER_KEY the test suite enables the \
notifications over secure channel'''))
            self.addEnvItem(('X509_CONSUMER_KEY','''location of the consumer private key, \
if this variable is set together with X509_CONSUMER_CERT the test suite enables the \
notifications over secure channel. The key must be protected by a passphrase.'''))

        
    def register(self, name, type, default=None, check=None, optChar=None, \
                 descr=''):
        
        if check==None:
            check = checkIsOk

        if optChar==None:
            optChar = name[0]
            
        self.pTable[name] = (type, check, optChar, descr)
        if default==None and type=='s':
            default=''
        elif default==None and type=='d':
            default=0
        elif default==None and type=='b':
            default=False

        setattr(self, name, default)

    def testAndSet(self, k, v):
        type, check, optChar, descr = self.pTable[k]
        if type=='d':
            iValue = int(v)
            setattr(self, k, iValue)
        elif type=='b':
            if v=='':
                setattr(self, k, True)
            else:
                setattr(self, k, v.lower()=='true')
        else:
            setattr(self, k, v)        
    
    def testValues(self):
        for param in self.pTable:
            type, check, optChar, descr = self.pTable[param]
            if type<>'b':
                check(getattr(self,param))

    def parseOptList(self, argv, checkParam=False):
        
        try:
            optList, args = getopt.getopt(argv, self.getShortOptString(), self.getLongOptList())
        except getopt.GetoptError, err:
            raise BadValueException("Wrong arguments: " + str(err))
        
        for k,v in optList:
            if k[0:2]=='--':
                optName = k[2:]
            elif k[0]=='-':
                for tmpo in self.pTable.keys():
                    type, check, optChar, descr = self.pTable[tmpo]
                    if optChar==k[1]:
                        optName = tmpo
            self.testAndSet(optName, v)
            
        if checkParam and not self.help:
            self.testValues()

    def parseConfigFile(self, confFileName, checkParam=False):
        propRegExp = re.compile("^\s*(\w+)\s*=\s*(\w+)")
        try:
            confFile = open(confFileName)
            for line in confFile:
                prop = propRegExp.search(line)
                if prop <> None:
                    self.testAndSet(prop.group(1), prop.group(2))
            confFile.close()
        finally:
            if 'confFile' in dir() and not confFile.closed:
                confFile.close()
                
        if checkParam and not self.help:
            self.testValues()
            
    def parseConfigFileAndOptList(self, optList, confFileName):
        try:
            if confFileName<>None:
                self.parseConfigFile(confFileName, False)
            self.parseOptList(optList, True)
        except BadValueException, ex:
            print "ERROR: %s\n\nUsage:" % ex
            self.display()
            sys.exit(1)
        except IOError, ioError:
            print "Cannot read configuration file: " + confFileName + " " + str(ioError)
            sys.exit(1)
        except Exception, exception:
            print "Error parsing parameters " + str(exception)
            sys.exit(1)
            
        if self.help:
            self.display()
            sys.exit(0)
    
        for k in cmdTable.keys():
            if not os.access(cmdTable[k], os.X_OK):
                print "Cannot find executable " + cmdTable[k]
                sys.exit(1)
            
    def getLongOptList(self):
        result = []
        for k in self.pTable.keys():
            type, check, optChar, descr = self.pTable[k]
            if type=='b':
                result.append(k)
            else:
                result.append(k + "=")
        return result

    def getShortOptString(self):
        result = ''
        for k in self.pTable.keys():
            type, check, optChar, descr = self.pTable[k]
            if type=='b':
                result += optChar
            else:
                result += (optChar + ":")
        return result

    def _convertParam(self, item):
        type, check, optChar, descr = self.pTable[item]
        if optChar=='':
            f1 = ''
        else:
            f1 = '-' + optChar
        f2 = '--' + item
        if type=='b':
            f3 = ''
        elif type=='d':
            f3 = 'NUMBER'
        else:
            f3 = 'STRING'
        return (f1, f2, f3, descr)
        
    def display(self):
        
        if os.environ.has_key("HELP_FORMAT"):
            format = os.environ["HELP_FORMAT"]
        else:
            format = None
            
        executable = os.path.basename(sys.argv[0])
        
        if format=='GROFF':
            print ".TH " + executable + ' "1" ' + executable + ' "GLITE Testsuite"\n'
            print ".SH NAME:"
            print executable + ' \- ' + self._shortDescr + '\n'
            print ".SH SYNOPSIS"
            print '.B ' + executable
            print self._synopsis + '\n'
            print '.SH DESCRIPTION'
            print self._description + '\n'
            if len(self.pTable)>0:
                print '.SH OPTIONS'
                sortedKeys = self.pTable.keys()
                sortedKeys.sort()
                for item in sortedKeys:
                    pTuple = self._convertParam(item)
                    if pTuple[3]=='':
                        continue
                    if pTuple[0]=='':
                        print '.HP\n%s\\fB%s\\fR\n%s\n\n.IP\n%s\n.PP' % pTuple
                    else:
                        print '.HP\n\\fB%s\\fR, \\fB%s\\fR\n%s\n\n.IP\n%s\n.PP' % pTuple
            if len(self._env)>0:
                print '.SH ENVIRONMENT'
                for item in self._env:
                    print '.TP\n.B %s\n%s\n.' % item
                    
        elif format=='TWIKI':
            print "---++ " + executable + "\n"
            print "   $ *NAME*: " + executable + " - " + self._shortDescr + "\n"
            print "   $ *SYNOPSIS*: " + executable + ' ' + self._synopsis + "\n"
            print "   $ *DESCRIPTION*: " + self._description + "\n"
            print "*OPTIONS*"
            sortedKeys = self.pTable.keys()
            sortedKeys.sort()
            for item in sortedKeys:
                pTuple = self._convertParam(item)
                if pTuple[3]=='':
                        continue
                tmps = string.strip('%s %s %s' % pTuple[:3])
                print "   * *" + tmps + "* " + pTuple[3] + "\n"
            if len(self._env)>0:
                print "*ENVIRONMENT*\n"
                for item in self._env:
                    print "   * *%s* %s\n" % item

        else:
            print "NAME\n\t" + executable + " - " + self._shortDescr + "\n"
            print "SYNOPSIS\n\t" + executable + ' ' + self._synopsis + "\n"
            print "DESCRIPTION\n\t" + self._description + "\n"
            sortedKeys = self.pTable.keys()
            sortedKeys.sort()
            for item in sortedKeys:
                pTuple = self._convertParam(item)
                if pTuple[3]=='':
                        continue
                if pTuple[0]=='':
                    print "\t%s%s %s\n\t\t%s\n" % pTuple
                else:
                    print "\t%s %s %s\n\t\t%s\n" % pTuple
            if len(self._env)>0:
                print "ENVIRONMENT\n"
                for item in self._env:
                    print "\t%s\n\t\t%s\n" % item

def createTempJDL(sleepTime):
    tempFD, tempFilename = tempfile.mkstemp(dir='/tmp')
    try:
        tFile = os.fdopen(tempFD, 'w+b')
        tFile.write('[executable="/bin/sleep";\n')
        tFile.write('arguments="' + str(sleepTime) +'";]\n')
        tFile.close()
        return tempFilename
    except :
        if mainLogger<>None:
            mainLogger.error("Cannot create temp jdl file:" + str(sys.exc_info()[0]))
    return None

def getProxyFile():
    if os.environ.has_key("X509_USER_PROXY"):
        credFile = os.environ["X509_USER_PROXY"];
    else:
        credFile = "/tmp/x509up_u" + str(os.getuid())
        
    if not os.path.isfile(credFile):
        raise Exception, "Cannot find proxy: " + credFile
    
    return credFile

def getUserKeyAndCert():
    if not os.environ.has_key("X509_USER_CERT") or \
        not os.environ.has_key("X509_USER_KEY"):
        return (None, None)
    
    cert = os.environ["X509_USER_CERT"]
    if not os.path.isfile(cert):
        raise Exception, "Cannot find: " + cert
    key = os.environ["X509_USER_KEY"]
    if not os.path.isfile(key):
        raise Exception, "Cannot find: " + key
    return (cert, key)

def getCACertDir():
    if os.environ.has_key("CACERT_DIR"):
        caCertDir = os.environ["CACERT_DIR"];
    else:
        caCertDir = "/etc/grid-security/certificates"
        
    if not os.path.isdir(caCertDir):
        raise Exception, "Cannot find CA certificate directory " + caCertDir
    return caCertDir
        
class Logger:
    
    def __init__(self):
        self.factory = None
        self.main = None
    
    def setup(self, confFile):

        # This is just a patch for registering the configuration file
        if confFile<>None and confFile<>'':
            log4py.CONFIGURATION_FILES[1] = confFile
        self.factory = log4py.Logger()
        self.main = self.factory.get_instance(classid="main")
        
    def get_instance(self, classid="main"):
        return self.factory.get_instance(classid=classid)
        
    def debug(self, message):
        self.main.debug(message)
        
    def error(self, message):
        self.main.error(message)
        
    def warn(self, message):
        self.main.warn(message)

    def info(self, message):
        self.main.info(message)
        
class InterfaceManager:
    
    def __init__(self):
        self.cond = threading.Condition()
        self.isRunning = True
        self.old_settings = termios.tcgetattr(sys.stdin.fileno())
        
    def run(self, jobTable=None):
        tty.setraw(sys.stdin.fileno())
        new_settings = termios.tcgetattr(sys.stdin.fileno())
        new_settings[1] = new_settings[1] | 1
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, new_settings)
        
        ch = ''
        while ch<>'S':
            ch = sys.stdin.read(1)
            if ch=='L' and jobTable<>None:
                print "---------------------------------- JOB ID LIST ----------------------------------"
                for job in jobTable.jobIdList():
                    print job
                print "---------------------------------------------------------------------------------"
        
        self.cond.acquire()
        self.isRunning = False
        self.cond.notify()
        self.cond.release()
            
    def sleep(self, timeToSleep):
        self.cond.acquire()
        self.cond.wait(timeToSleep)
        result = self.isRunning
        self.cond.release()
        return result
    
    def close(self):
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self.old_settings)

if 'testsuite_utils' in __name__:
    global cmdTable
    if os.environ.has_key("GLITE_LOCATION"):
        gliteLocation = os.environ["GLITE_LOCATION"]
    else:
        gliteLocation = "/opt/glite"
        
    cmdTable = { "submit": gliteLocation + "/bin/glite-ce-job-submit",
                       "status": gliteLocation + "/bin/glite-ce-job-status",
                       "cancel": gliteLocation + "/bin/glite-ce-job-cancel",
                       "purge": gliteLocation + "/bin/glite-ce-job-purge",
                       "subscribe": gliteLocation + "/bin/glite-ce-monitor-subscribe",
                       "unsubscribe": gliteLocation + "/bin/glite-ce-monitor-unsubscribe",
                       "lease": gliteLocation + "/bin/glite-ce-job-lease",
                       "proxy-init": gliteLocation + "/bin/voms-proxy-init",
                       "proxy-info": gliteLocation + "/bin/voms-proxy-info",
                       "delegate": gliteLocation + "/bin/glite-ce-delegate-proxy",
                       "proxy-renew": gliteLocation + "/bin/glite-ce-proxy-renew",
                 "event": gliteLocation + "/bin/glite-ce-event-query"};

    global hostname
    proc = popen2.Popen4('/bin/hostname -f')
    hostname =  string.strip(proc.fromchild.readline())
    proc.fromchild.close()
    
    global applicationTS
    applicationTS = time.time()
    global applicationID
    applicationID = "%d.%f" % (os.getpid(), applicationTS)
    
    global mainLogger
    mainLogger = Logger()
    
    global failureReList
    failureReList = []
    if os.environ.has_key("FAILURE_FILE"):
        failureFilename = os.environ["FAILURE_FILE"]
    else:
        failureFilename = ".failureRegEx"
        
    if os.path.exists(failureFilename):
        failureFile = None
        try:
            failureFile = open(failureFilename)
            for line in failureFile:
                regex = string.strip(line)
                if len(regex)>0:
                    failureReList.append(re.compile(regex))
        finally:
            if failureFile<>None:
                failureFile.close()
            

else:
    print __name__
