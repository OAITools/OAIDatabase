#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
Convert Osteoarthritis Initiative (OAI) 
SAS Data to Postgres SQL files
---------------------------------------------------

This is a bit tedious. OAI data is distributed in flat files and SAS files,
but it would be more convenient to have a database back-end, representing
data sets as views.

SAS files (sas7bdat files) can be read using the  Python library 
sas7bdat-2.0.5 which gives us access to column data format and descriptions.

Since metadata like categories and subcategories isn't provided in non-PDF
form, we convert PDFs to text, then use a script to generate a dictionary
of metadata

DEPENDANCES:
* sas7bdat-2.0.5    https://pypi.python.org/pypi/sas7bdat
* pdftotext v3.04   http://www.foolabs.com/xpdf/download.html 

@author: Jason Alan Fries <jfries [at] stanford.edu>

'''

import glob
import zipfile
import os
import sys
import subprocess
import operator
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
    variable names and descriptions. This is a rather ugly hack.
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
    '''Parse data out of variable description PDF (VG_Variable.pdf) provided 
    by the OAI. File is converted to text using:
    
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
                
        # Fix labels for complete records. Ignore empty records.
        # Categories can be duplicated, so ensure labels are unique.
        if sum([1 if v else 0 for v in record.values()]):
            record["label"] = re.sub("\s{2,}"," ",record["label"])
            record["comments"] = re.sub("\s{2,}"," ",record["comments"])
            record["category"] = {x:1 for x in record["category"]}.keys()
            record["subcategory"] = {x:1 for x in record["subcategory"]}.keys()
            datdict[record["id"]] = record

    return datdict


def sql_populate_metadata(metadata):
    '''Generate two SQL tables using hand-coded schema from oai.sql
        1. Category information
        2. Dataset information
        
    '''
    CATEGORY = 1
    SUBCATEGORY = 2
    
    cat_schema = "INSERT INTO categories (var_name, cat_type, cat_name) VALUES"
    dat_schema = "INSERT INTO datasets (var_name, dataset, collect_form, comment) VALUES"
    
    categories = []
    datasets = []
    
    # populate tables
    for var_name in metadata:
        
        for cat in metadata[var_name]["category"]:
            categories += ["\t('%s', %s, '%s')" % (var_name, CATEGORY, cat)]
        
        for subcat in metadata[var_name]["subcategory"]:
            categories += ["\t('%s', %s, '%s')" % (var_name, SUBCATEGORY, subcat)]
        
        ds = "'%s'" % metadata[var_name]["dataset"]
        collect = "'%s'" % metadata[var_name]["collection"]
        cmmnt = "NULL" if metadata[var_name]["comments"] == "None" else "'%s'" % metadata[var_name]["comments"]
        datasets += [(var_name, ds, collect, cmmnt)]
        
    # sort by dataset
    datasets = sorted(datasets,key=operator.itemgetter(1),reverse=0)
    datasets = map(lambda x:"\t('%s', %s, %s, %s)" % x,datasets)
    
    print "%s\n%s;" % (cat_schema, ",\n".join(categories))
    print
    print "%s\n%s;" % (dat_schema, ",\n".join(datasets))
    
def sql_dataset_schema(tbl_name, sasheader, metadata):
    '''
    '''
    dtypes = {"MMDDYY":"DATE","":"NUMERIC","$":"TEXT"}
    manual = {"VERSION":"TEXT"}
    notnull = {"ID":1,"VERSION":1}
    
    #
    # 1. Create Table Schema (using SAS format field)
    #
    sql = []
    for i, col in enumerate(sasheader.parent.columns):
        null = " NOT NULL" if col.name in notnull else ""
        dtype = "NUMERIC"
        if col.format in dtypes:
            dtype = dtypes[col.format]
        if col.name in manual:
            dtype = manual[col.name]

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
    for i, col in enumerate(sasheader.parent.columns):
        var = col.name
        # only include column defs for this table
        if var not in metadata:
            continue
        s = "COMMENT ON column %s.%s is '%s';" % (tbl_name,var,metadata[var]["label"])
        sql += [s]
    
    comment_sql = "\n".join(sql)
    
    print table_sql
    print comment_sql
    
    

def sql_insert(tbl, schema, values):
    '''Helper function for generating sql
    '''
    sql = "INSERT INTO %s (%s) VALUES" % (tbl, ", ".join(schema))
    
    rows = []
    for row in values:
        row = tuple(["%s" % v if v else "NULL" for v in row])
        rows += ["\t%s" % (row,)]
        
    return "%s\n%s;" % (sql, ",\n".join(rows))

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
    
     
def main():
    
    #
    # 1. Category/sub-category information 
    #
    metatdata = load_metadata("../data/VG_Variable_tables.bz2")
    sql_populate_metadata(metatdata)
    
    rootdir = "/Users/fries/Desktop/data/"
    dirs = [x for x in os.listdir(rootdir) if not os.path.isfile(rootdir+x)]
  
    #
    # load SAS files
    #
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
        tbl_name = datafile.split(".")[0]
        sql_dataset_schema(tbl_name, d.header, metatdata)
        
        
        continue
        
        # SAS data: create database INSERT statements
        rows = [row for row in d]
        schema = rows[0]
        sql_insert(tbl_name,schema,rows[1:])
        
        
        
           
    
main()