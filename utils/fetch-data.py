#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
Download Osteoarthitis Initiative (OAI) Datasets
---------------------------------------------------

Osteoarthitis Initiative data is available upon completing a 
DUA at:

@author: Jason Alan Fries <jfries [at] stanford.edu>

'''
import sys
import getpass
import argparse
import urllib2


import httplib
from httplib import HTTPConnection, HTTPS_PORT
import ssl
import socket

# There is a SSL bug that generates errors of the form
# ssl.SSLEOFError: EOF occurred in violation of protocol (_ssl.c:590)
# This hack fixes those issues
class HTTPSConnection(HTTPConnection):
    "This class allows communication via SSL."
    default_port = HTTPS_PORT

    def __init__(self, host, port=None, key_file=None, cert_file=None,
            strict=None, timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
            source_address=None):
        HTTPConnection.__init__(self, host, port, strict, timeout,
                source_address)
        self.key_file = key_file
        self.cert_file = cert_file

    def connect(self):
        "Connect to a host on a given (SSL) port."
        sock = socket.create_connection((self.host, self.port),
                self.timeout, self.source_address)
        if self._tunnel_host:
            self.sock = sock
            self._tunnel()
        # this is the only line we modified from the httplib.py file
        # we added the ssl_version variable
        self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_TLSv1)

#now we override the one in httplib
httplib.HTTPSConnection = HTTPSConnection
# ssl_version corrections are done



BASE_URL = "https://oai.epi-ucsf.org/datarelease/DataAgreementCheck.asp?file=%s"

def main(args):
    
    datasets = ["AllClinical%s_SAS.zip","MIF%s_SAS.zip","MRI%s_SAS.zip",
                "Xray%s_SAS.zip"]
    datasets_docs = ["AllClinical%s_Doc.zip","MIF%s_Doc.zip","MRI%s_Doc.zip",
                     "Xray%s_Doc.zip"]
    

    # get login password
    pw = getpass.getpass()
   
    # build authentication
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor())
    urllib2.install_opener(opener)

    #https://oai.epi-ucsf.org/datarelease/UserAccountHome.asp
    auth_url="https://oai.epi-ucsf.org/datarelease/UserLogonFunction.asp"
    
    c = httplib.HTTPSConnection("oai.epi-ucsf.org")
    
    
    c.request("GET", "/datarelease/")
    response = c.getresponse()
    
    print response.status, response.reason
    data = response.read()
    print data
        
    
    '''
    try:
        response = urllib2.urlopen('https://oai.epi-ucsf.org') 
        print 'response headers: "%s"' % response.info()
    except IOError, e:
        if hasattr(e, 'code'): # HTTPError
            print 'http error code: ', e.code
        elif hasattr(e, 'reason'): # URLError
            print "can't connect, reason: ", e.reason
        else:
            raise
    '''
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-u","--username", type=str, help="OAI username")
    args = parser.parse_args()
    
    # argument error, exit
    if not args.username:
        parser.print_help()
        sys.exit()
    
    main(args)
    


