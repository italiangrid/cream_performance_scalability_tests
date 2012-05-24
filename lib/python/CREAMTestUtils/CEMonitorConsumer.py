from ZSI import ParsedSoap, SoapWriter, resolvers, TC, \
    FaultFromException, FaultFromZSIException
import os, sys, glob
from SocketServer import ThreadingMixIn
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from OpenSSL import SSL, tsafe, crypto
import testsuite_utils, job_utils
import re, string
import select, socket

NSTable = {'cemon_types': 'http://glite.org/ce/monitorapij/types', \
                    'cemon_faults': 'http://glite.org/ce/monitorapij/faults', \
                    'cemon_consumer': 'http://glite.org/ce/monitorapij/ws'}

jobIDRE = re.compile('CREAM_JOB_ID\s*=\s*\"(CREAM[0-9E-]+)\"\s*;')
jobStatusRE = re.compile('JOB_STATUS\s*=\s*\"([A-Z-]+)\"\s*;')
jobFailureRE = re.compile('FAILURE_REASON\s*=\s*([^;]+);')

class Notify:
    def __init__(self):
        pass
Notify.typecode = TC.Struct(Notify, [], 'NotifyResponse')

class SOAPRequestHandler(BaseHTTPRequestHandler):

    server_version = 'CEMonitorConsumer/1.8 ' + BaseHTTPRequestHandler.server_version
    
    def send_xml(self, text, code=200):

        self.send_response(code)
        self.send_header('Content-type', 'text/xml; charset="utf-8"')
        self.send_header('Content-Length', str(len(text)))
        self.end_headers()
        self.wfile.write(text)
        self.wfile.flush()

    def send_fault(self, f, code=500):

        self.send_xml(f.AsSOAP(), code)
        
    def log_message(self, format, *args):
        ConsumerServer.logger.debug(format%args)

    def handleNotify(self, soap):
    
        nlist = soap.getElementsByTagNameNS(NSTable['cemon_types'], 'Notification')
        if len(nlist)==0:
            return

        events = nlist[0].getElementsByTagNameNS(NSTable['cemon_types'], 'Event')
        for event in events:
            jobHistory = []
            
            messages = event.getElementsByTagNameNS(NSTable['cemon_types'], 'Message')
            for msg in messages:
                if len(msg.childNodes)==0:
                    continue
                classad = msg.childNodes[0].nodeValue
                r1 = jobIDRE.search(classad)
                r2 = jobStatusRE.search(classad)
                r3 = jobFailureRE.search(classad)
                if r3<>None:
                    failureReason = r3.group(1)
                else:
                    failureReason = ''

                if r1<>None and r2<> None:
                    jobHistory.append( (self.server.servicePrefix+r1.group(1), \
                                                    r2.group(1), failureReason) )
                elif 'KEEP_ALIVE' in classad:
                    ConsumerServer.logger.debug('Keep alive notification')
                else:
                    ConsumerServer.logger.debug('Irregular classad: ' + classad)
                    
            if len(jobHistory)>0:
                self.server.jobTable.notify(jobHistory)


    def do_POST(self):

        try:
            ct = self.headers['content-type']
            if ct.startswith('multipart/'):
                cid = resolvers.MIMEResolver(ct, self.rfile)
                xml = cid.GetSOAPPart()
                ps = ParsedSoap(xml, resolver=cid.Resolve)
            else:
                length = int(self.headers['content-length'])
                ps = ParsedSoap(self.rfile.read(length))
        except ParseException, e:
            ConsumerServer.logger.error(str(e))
            self.send_fault(FaultFromZSIException(e))
            return
        except Exception, e:
            ConsumerServer.logger.error(str(e))
            self.send_fault(FaultFromException(e, 1, sys.exc_info()[2]))
            return
        
        try:
            self.handleNotify(ps.body_root)
            #TODO: missing namespace cemon_consumer
            sw = SoapWriter(nsdict=NSTable)
            sw.serialize(Notify(), Notify.typecode)
            sw.close()
            self.send_xml(str(sw))
        except Exception, e:
            ConsumerServer.logger.error(str(e))
            import traceback
            traceback.print_tb(sys.exc_info()[2])
            self.send_fault(FaultFromException(e, 0, sys.exc_info()[2]))
            
class ConnectionFixer(object):
    """ wraps a socket connection so it implements makefile """
    def __init__(self, conn):
        self.__conn = conn
    def makefile(self, mode, bufsize):
       return socket._fileobject(self.__conn, mode, bufsize)
    def __getattr__(self, attrib):
        return getattr(self.__conn, attrib)
    
class TSafeConnection(tsafe.Connection):
    def settimeout(self, *args):
        self._lock.acquire()
        try:
            return self._ssl_conn.settimeout(*args)
        finally:
            self._lock.release()


class ConsumerServer(ThreadingMixIn, HTTPServer):
    #See description in SocketServer.py about ThreadingMixIn
    
    logger = None
    
    def __init__(self, address, parameters, jobTable=None, proxyMan=None):
        HTTPServer.__init__(self, address, SOAPRequestHandler)
        
        if ConsumerServer.logger==None:
            ConsumerServer.logger = testsuite_utils.mainLogger.get_instance(classid='ConsumerServer')
        
        if os.environ.has_key("X509_CONSUMER_CERT") and \
            os.environ.has_key("X509_CONSUMER_KEY"):
            self.consumerCert = os.environ["X509_CONSUMER_CERT"]
            if not os.path.isfile(self.consumerCert):
                raise Exception, "Cannot find: " + self.consumerCert
            self.consumerKey = os.environ["X509_CONSUMER_KEY"]
            if not os.path.isfile(self.consumerKey):
                raise Exception, "Cannot find: " + self.consumerKey
            
            if testsuite_utils.checkEncryptedKey(self.consumerKey):
                import getpass
                self.password = getpass.getpass('Password for consumer key: ')
            else:
                self.password = ''
#        elif proxyMan<>None and proxyMan.key<>None:
#                self.consumerCert = proxyMan.cert
#                self.consumerKey = proxyMan.key
#                self.password = proxyMan.password
        else:
            self.consumerCert = None
            self.consumerKey = None
            self.password = None
        
        if self.consumerKey<>None:
            ConsumerServer.logger.debug("Enabled secure channel for notifications")
            self.ssl_context = SSL.Context(SSL.SSLv23_METHOD)
            buffer = self.readPEMFile(self.consumerKey)
            if self.password<>'':
                privateKey = crypto.load_privatekey(crypto.FILETYPE_PEM, 
                                                    buffer, self.password)
            else:
                privateKey = crypto.load_privatekey(crypto.FILETYPE_PEM, buffer)
            self.ssl_context.use_privatekey(privateKey)
            self.ssl_context.use_certificate_file(self.consumerCert)
            caStore = self.ssl_context.get_cert_store()
            caList = glob.glob(testsuite_utils.getCACertDir() + "/*.0")
            for item in caList:
                buffer = self.readPEMFile(item)
                caCert = crypto.load_certificate(crypto.FILETYPE_PEM, buffer)
                caStore.add_cert(caCert)
            
            tmpsock = socket.socket(self.address_family,self.socket_type)
            self.socket = TSafeConnection(self.ssl_context, tmpsock)
            self.socket.settimeout(30)
            self.server_bind()
            self.server_activate()
        else:
            self.ssl_context = None
        
        self.jobTable = jobTable
        self.parameters = parameters
        self.running = False
        self.servicePrefix = 'https://' + parameters.resourceURI[:string.find(parameters.resourceURI,'/') + 1]
        self.cemonURL = self.servicePrefix + "ce-monitor/services/CEMonitor"
        
        self.proxyFile = testsuite_utils.getProxyFile()
        self.subscrId = job_utils.subscribeToCREAMJobs(self.cemonURL, \
                                                       self.parameters, self.proxyFile, self.ssl_context<>None)
        
    def readPEMFile(self, filename):
        tmpf = file(filename)
        buffer = ''
        for line in tmpf:
            buffer += line
        tmpf.close()
        return buffer
    
    def get_request(self):
        if self.ssl_context:
            (conn, info) = self.socket.accept()
            conn = ConnectionFixer(conn)
            return (conn, info)
        
        return self.socket.accept()

    def __call__(self):
        self.running = True
        sList = [self]
        dList = []
        while self.running:
            list1, list2, list3 = select.select(sList, dList, dList, 5)
            if len(list1)>0:
                self.handle_request()
        ConsumerServer.logger.debug("Consumer thread is over")
            
    def halt(self):
        job_utils.unSubscribeToCREAMJobs(self.cemonURL, self.subscrId, \
                                         self.parameters, self.proxyFile)
        self.running = False

class DummyTable:
    
    def __init__(self, uri, port , rate):
        self.consumerPort = port
        self.rate = rate
        self.resourceURI = uri
        
    def notify(self, jobHistory):
        print '---------------------------------------------'
        for item in jobHistory:
            print "JobID: %s status: (%s, %s)" % item

def main():
    import time
    import threading
    testsuite_utils.mainLogger.setup("log4py.conf")
    dummyObj = DummyTable(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
    consumer = ConsumerServer(('',int(sys.argv[2])), dummyObj, dummyObj)
    consumerThread = threading.Thread(target=consumer)
    consumerThread.start()
    time.sleep(int(sys.argv[3])*5)
    consumer.halt()

if __name__ == "__main__":
    main()


