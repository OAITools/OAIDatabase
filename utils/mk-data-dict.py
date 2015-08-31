#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
Initialize Osteoarthritis Initiative (OAI) Database
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
import bz2
from subprocess import Popen, PIPE
import re
import sas7bdat
from _sqlite3 import Row

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
        
        
def load_metadata(filename):
    '''Ugly hack for parsing data out of variable description PDF
    (VG_Variable.pdf) provided by the OAI. File is converted to text using
    pdftotext -table <INPUT> <OUTPUT>
    This file gives us category, subcategory, and data set information, which
    I can't seem to locate as independant data sets on the OAI web site.
    
    '''
    datdict = {}
    
    # load text file (compressed or plain text)
    if filename.split(".")[-1] == "bz2":
        datfile = bz2.BZ2File(filename,"rb")
    else:
        datfile = open(filename,"rU")
        
    with datfile:
        data = datfile.readlines()
    
    data = "".join(data)
    pages = re.split("[_]{2,}",data)
    
    regex = {}
    regex["label"] = re.compile("Label:(.*)")
    regex["collection"] = re.compile("Data Collection Form:(.+)")
    regex["dataset"] = re.compile("SAS Dataset:(.+)")
    regex["comments"] = re.compile("Release Comments:(.+)")
    regex["category"] = re.compile("Category\s+SubCategory")
    
    for item in pages:
        
        lines = item.split("\n")
        lines = [x for x in lines if x.strip()]
  
        record = {"id":None, "label":None, "dataset":None, "comments": None}
        record["collection"] = None
        record["category"] = []
        record["subcategory"] = []
        record["cat-span"] = None
        
        for i,line in enumerate(lines):
            
            if re.match("\s+",line[0])  and not record["id"]:
                continue
            
            if not record["id"]:
                record["id"] = line.strip()
                continue
            
            if record["label"] and not record["collection"] and \
            re.match("\s+",line[0]):
                record["label"] += line
                
            for field in regex:
                m = regex[field].search(line)
                if m and field != "category":
                    record[field] = m.group(1).strip()
        
            # extract categories and subcategories
            m = regex["category"].search(line)
            if m:
                offset = m.group().index("SubCategory")
                record["cat-span"] = offset
                reject = ["Cumulative","N"] # reject lines containing this words
                
                for j in range(i+1, len(lines)):
                    
                    if set(lines[j].split()).intersection(reject) or \
                    lines[j][0] == " ":
                        break
                    
                    record["category"] += [lines[j][0:record["cat-span"]].strip()]
                    record["subcategory"] += [lines[j][record["cat-span"]:].strip()]
                
                break
                
        # Fix labels for complete records. Ignore empty records
        if sum([1 if v else 0 for v in record.values()]):
            record["label"] = re.sub("\s{2,}"," ",record["label"])
            record["comments"] = re.sub("\s{2,}"," ",record["comments"])
            datdict[record["id"]] = record

    return datdict


def metadata_schema(metadata):
    '''Generate two SQL tables:
    1) Category information
    2) Dataset information
    '''
    return
    for varname in metadata:
        print metadata[varname]



    
def dataset_schema(tbl_name, data, metadata):
    '''
    '''
    dtypes = {"MMDDYY":"DATE","":"NUMERIC","$":"TEXT"}
    manual = {"VERSION":"TEXT"}
    notnull = {"ID":1,"VERSION":1}
    
    #
    # 1. Create Table Schema (using SAS format field)
    #
    sql = []
    #col_idx = {}
    for i, col in enumerate(data.parent.columns):
        null = " NOT NULL" if col.name in notnull else ""
        dtype = "NUMERIC"
        if col.format in dtypes:
            dtype = dtypes[col.format]
        if col.name in manual:
            dtype = manual[col.name]
        
        #col_idx[col.name] = 1
        sql += ["\t%s %s%s" % (col.name, dtype, null)]
    
    sql += ["PRIMARY KEY(ID)"]
    table_sql = "CREATE TABLE %s" % tbl_name
    table_sql = table_sql + "(\n%s );"
    table_sql = table_sql % ",\n".join(sql)
    
    #
    # 2. Label Columns
    #
    sql = []
    #for var in metadata:
    for i, col in enumerate(data.parent.columns):
        var = col.name
        # only include column defs for this table
        if var not in metadata:
            continue
        s = "COMMENT ON column %s.%s is '%s';" % (tbl_name,var,metadata[var]["label"])
        sql += [s]
    
    comment_sql = "\n".join(sql)
    
    return 

def create_sql_schema(header,metadata):
    
    d = {}
    dtypes = {}
    dtypes = {"MMDDYY":"DATE","":"NUMERIC"}
  
    
    for i, col in enumerate(header.parent.columns):
        #print [i, col.name, col.type, col.length, col.format, col.label]
        col.name = col.name.upper()
        d[col.name] = {"col":col, "category":[], "subcategory":[]}
        
    
    for name in d:  
        print name, d[name]["col"].format
    

'''
CREATE TABLE AppDoc (
        AppDocID INTEGER NOT NULL,
        ApplNo VARCHAR(6) NOT NULL,
        SeqNo VARCHAR(4) NOT NULL,
        DocType VARCHAR(50) NOT NULL,
        DocTitle VARCHAR(100),
        DocURL VARCHAR(200),
        DocDate DATE,
        ActionType VARCHAR(10) NOT NULL,
        DuplicateCounter INTEGER,
        PRIMARY KEY(AppDocID) );
'''
        
def main():
    
    # Load category/subcategory information 
    metatdata = load_metadata("../data/VG_Variable_tables.bz2")
    
    metadata_schema(metatdata)
    
    
    # load SAS files
    indir = "/Users/fries/Desktop/data/clinical/AllClinical*_SAS.zip"
    filelist = glob.glob(indir)
    
    for zfname in filelist:
        zf = zipfile.ZipFile(zfname, 'r')
        manifest = sorted(zf.namelist())
        datafile = [x for x in manifest if "sas7bdat" in x][0]

        data = zf.read(datafile)
        tmpfile = "/tmp/temp.sas7bdat"
        
        # dump PDF to a temporary file
        with open(tmpfile,"wb") as tmp:
            tmp.write(data)
        
        
        d = sas7bdat.SAS7BDAT(tmpfile)
        
        # SAS metadata (contains label, format)
        tbl_name = datafile.split(".")[0].upper()
        dataset_schema(tbl_name, d.header, metatdata)
        
        #table_sql = "CREATE TABLE AppDoc (\n%s,\nPRIMARY KEY(AppDocID));"
        #table_sql = table_sql % dataset_schema(d.header,metatdata)
        #print table_sql
        sys.exit()
        
        for i,row in df.iterrows():
            print i
            for value in row:
                print value, type(value)
        
           
    
main()