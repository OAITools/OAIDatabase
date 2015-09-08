#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''

TODO -- need to sort out https authentication using
form data + httplib. Until then, we just have to 
manually download all datasets. 

---------------------------------------------------
Download Osteoarthitis Initiative (OAI) Datasets
---------------------------------------------------

Osteoarthitis Initiative data is available upon completing a 
DUA at:

@author: Jason Alan Fries <jfries [at] stanford.edu>

'''
import sys
import os
import getpass
import argparse
import urllib2

import mechanize
import cookielib
import urllib

import httplib
from httplib import HTTPConnection, HTTPS_PORT
import ssl
import socket

# There is a SSL bug that generates errors of the form
# ssl.SSLEOFError: EOF occurred in violation of protocol (_ssl.c:590)
# This hack fixes those issues
# ====================================================================
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
# ====================================================================

FILE_URL = "https://oai.epi-ucsf.org/datarelease/DataAgreementCheck.asp?file="

def authenticate(username,password):
    
    # authenticate
    browser = mechanize.Browser()
    cookies = cookielib.LWPCookieJar()
    browser.set_cookiejar(cookies)
   
    browser.set_handle_equiv(True)
    browser.set_handle_gzip(True)
    browser.set_handle_redirect(True)
    browser.set_handle_referer(True)
    browser.set_handle_robots(False)

    browser.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)
    OAI_URL = "https://oai.epi-ucsf.org/datarelease/UserLogonFunction.asp"
    r = browser.open(OAI_URL)

    # Login using form 
    browser.select_form(nr=0) # first (only) form on the page
    browser.form["Username"] = username
    browser.form["Password"] = password
    
    browser.submit()
    
    return browser

def main(args):
    
    # load data set list
    datasets = map(lambda x:x.strip(), open("datasets.txt","rU").readlines())
    datasets = [x for x in datasets if x and x[0] != "#"]
    
    # login/password
    if not args.username:
        args.username = raw_input("Login: ")
    
    pw = getpass.getpass()
    browser = authenticate(args.username,pw)
    
    print("Downloading datasets...")
    for filename in datasets:
        outfile = "%s%s" % (args.outputdir, filename)
        url = "%s%s" % (FILE_URL, filename)
        sys.stdout.write(" (+) %s..." % filename)
        browser.retrieve(url,outfile)
        sys.stdout.write("DONE\n")
        
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-u","--username", type=str, help="OAI username")
    parser.add_argument("-o","--outputdir", type=str, help="Data output dir")
    args = parser.parse_args()
    
    # argument error, exit
    if not os.path.exists(args.outputdir):
        parser.print_help()
        sys.exit()
        
    if args.outputdir[-1] != "/":
        args.outputdir = args.outputdir + "/"
        
    main(args)
    


