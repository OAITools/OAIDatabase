#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
Initialize Osteoarthitis Initiative (OAI) Database
---------------------------------------------------

This is pretty tedious. SAS XPORT files (*.xpt files) don't appear to have an 
open source reader implementation, at least not the compressed V8 variety. The 
Python library xport fails to read OAI xpt files, meaning we can't use 
native SAS column information to generate our database schema. 

Instead, we make use of the PDFs provided that describe data format and labels.
We convert PDFs to text, then use a script to generate a data dictionary of
column names, data type, and label. This dictionary is used by init-db.py
to generate our table schema.


@author: Jason Alan Fries <jfries [at] stanford.edu>

'''

import glob
import zipfile
import os
import sys
import subprocess
from subprocess import Popen, PIPE

PDF2TEXT = "/usr/local/bin/pdftotext"

def pdf2text(data,layout=True,tmpdir="/tmp"):
    ''' Convert PDF document to plain text file using external pdftotext
    command line utility. 
    
    '''
    pdffile = "%s/contents.pdf" % (tmpdir)
    txtfile = "%s/contents.txt" % (tmpdir)
    
    # dump PDF to a temporary file
    with open(pdffile,"wb") as tmp:
        tmp.write(data)
    
    # convert it to plain text
    cmd = "%s -layout %s %s" % (PDF2TEXT, pdffile, txtfile)
    os.system(cmd)

    # read in text file contents
    with open(txtfile) as f:
        txt = "".join(f.readlines())
    
    # clean up
    os.remove(pdffile)
    os.remove(txtfile)
    
    return txt


def convert_text_file(txt):
    ''' Convert a OAI data description file into a mapping of 
    variable names and descriptions. This is a rather ugly hack since
    we can't seem to read compressed V9 XPT files directly in Python :(
    '''
    
    # split by page
    data = []
    
    #page_type = "Alphabetic List of Variables and Attributes"
    page_type = "Variables in Creation Order"
    
    pages = txt.split("The CONTENTS Procedure")[1:]
    pages = [x for x in pages if page_type in x]
    
    
    # parse each page
    for page in pages:
        rows = page.split("\n")
        
        # strip header
        for i,line in enumerate(rows):
            line = line.strip()
            if line and line[0] == "#":
                break
            
        rows = rows[i:]
        
        # strip footer
        for i,line in enumerate(rows):
            if "OAI Version" in line:
                break
            
        rows = rows[0:i]
        rows.pop(0)
        
        # create rows
        mrows = []
        while rows:
            line = rows.pop(0)
            fields = line.strip().split()
            if not fields:
                continue
            
            if (fields[0].isdigit() and len(fields) > 5):
                mrows = mrows + [fields]
            else:
                mrows[-1] += fields
        
        
        for x in mrows:
            print x
            id, name, dtype, width = x[:4]
            
            # format details
            x = x[4:]
            fmt = []
            while True:
                col = x.pop(0)
                if col[-1] == ".":
                    fmt += [col]
                elif col.strip() in ["5.1"]:
                    fmt += [col]
                else:
                    x = [col] + x
                    break
            
            fmt = fmt + ["None","None"]
            fmt = fmt[0:2]
            label = " ".join(x)
            
            row = [id,name,dtype,width] + fmt + [label]
            data += [row]
            
    return data
        
def main():
    
    indir = "/Users/fries/Desktop/data/clinical/AllClinical*_Doc.zip"
    
    filelist = glob.glob(indir)
   
    for zfname in filelist:
        zf = zipfile.ZipFile(zfname, 'r')
        digest = sorted(zf.namelist())
        vardefs= [x for x in digest if "Contents" in x][0] # column defs file
        
        # convert PDF to text file
        pdf = zf.read(vardefs)
        txt = pdf2text(pdf)
        
        print vardefs
        data = convert_text_file(txt)
     
        print vardefs,  data[0:2],len(data)

main()