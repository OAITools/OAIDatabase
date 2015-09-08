#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
Convert Osteoarthritis Initiative (OAI) 
SAS Data to Postgres SQL files
---------------------------------------------------

This is an ugly script. OAI data is distributed in flat files and SAS files,
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


CAVEATS:
The X-ray,MRI, and MIF data sets have multiple records per subject and their
documentation states that a tuple of the form
    X-ray   (ID, EXAMTP, XRBARCD) 
    MIF     (ID, MEXAMTP, MRBARCD) 
    MRI     (ID, MEXAMTP, MRBARCD) 
uniquely identifies rows. *However* this isn't true in cases where subjects
did not receive an Xray, MRI, or MIF. In this case, a non-unique dummy row is
inserted into the table.

As a hack/fix, we remove the primary key constraint for these tables. 


@author: Jason Alan Fries <jfries [at] stanford.edu>

'''

import glob
import zipfile
import os
import sys
import subprocess
import operator
import bz2
import re
import sas7bdat
import argparse

PDF2TEXT = "/usr/local/bin/pdftotext"
ROW_INSERT_MAX = 5000

def psql_esc_str(s):
    return s.replace("'","''")

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
                           
def table_parser(rows):
    ''' Parse out category and variable types tables.
    More ugly code hacks to pull out metadata.
    '''
    #
    # 1: Category table
    #
    table_data ={}
    table_data["category"] = []
    table_data["subcategory"] = []
    
    header = rows.pop(0)
    offset = header.index("SubCategory")
    reject = ["Cumulative","N"] # reject lines containing this words
    
    for i in range(0,len(rows)):
        
        line = rows[i]
        if set(line.split()).intersection(reject) or line[0] == " ":
            break
        
        table_data["category"] += [line[0:offset].strip()]
        table_data["subcategory"] += [line[offset:].strip()]
    
    #
    # 2: Data Type 
    #
    dtable = rows[i:]
    
    table_data["type"] = "$" #unique
    table_data["label_n"] = 0
    table_data["values"] = None
    #table_data["label_names"] = None
    
    # no info about values -- this means a unique identifier
    if len(dtable) == 1:
        return table_data
   
    header = re.split("\s{2,}",dtable.pop(0))
    header = [x for x in re.split("\s{2,}",line) if x]
    
    if set(["Min","Max","Std Dev"]).intersection(header):
        table_data["type"] = "Continuous"
        table_data["label_n"] = 0
        return table_data

    ftable = []
    for line in dtable:
        line = line.replace("''  :","'':")
        values = [x for x in re.split("\s{2,}",line) if x]
        
        # HACK -- not enough spaces to correctly delimit 
        # manually split first column
        if len(values) != len(header):
            
            # manually identified case errors
            terms = ["OARSI","years","only","increase","Very"]
            if sum([1 for x in terms if x in values[1]]):
                values = ["%s %s" %(values[0], values[1])] + values[2:]
            else:
                m = re.search("\d*[,]*\d+$",values[0])
                if m:
                    col = m.group(0)
                    values[0] = values[0].replace(col,"").strip()
                    values.insert(1,col)    
                else:
                    print "FATAL ERROR -- NO MATCH"
                    sys.exit()
            
        # variable type (remove rows that correspond to tables that
        # span pages
        if values != ['Value', 'N', '%', 'Cumulative N', 'Cumulative %']:
            ftable += [values]
        
    table_data["type"] = "Nominal"
    labels = [re.match("^(.*):",x[0]) for x in ftable]
    
    # class labels are *not* numbered
    if len(labels) == labels.count(None):
        labels = [x[0] for x in ftable]
        table_data["values"] = "|".join(labels)
        table_data["label_n"] = len(labels)
        
    elif None in labels:
        
        labels = [x.group(0).replace(":","").replace("'","") if x else "" for x in labels]
        table_data["values"] = "|".join(map(str,labels))
        table_data["label_n"] = len(labels)
    
    else:
        # create class labels. For simplicities sake we 
        # don't assume an total ordering on these classes,
        # though several variables do have ordinal scales. 
        labels = [x.group(0).replace(":","").replace("'","") for x in labels]
        labels = [int(x) if x.isdigit() else None for x in labels]
        labels = {x:1 for x in labels}.keys()
        table_data["values"] = "|".join(map(str,labels))
        table_data["label_n"] = len(labels)
        
    return table_data

def is_header_footer(s):
    return re.search("Page \d+ of \d+",s) or re.search("Release Version",s) or \
        re.search("Variable Guide",s)
                
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
        
        # create page item
        # remove garbage lines (i.e., page numbers and footers)
        lines = item.split("\n")    
        lines = [x for x in lines if x.strip() and not is_header_footer(x)]
        
        record = {"id":None, "label":None, "dataset":None, "comments": None}
        record["collection"] = None
        record["category"] = []
        record["subcategory"] = []
        
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
                
                table_data = table_parser(lines[i:])
                record.update(table_data)
                 
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

metadata_schema = '''
CREATE TABLE categorydefs (
    id SERIAL,
    type INTEGER NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY(id) );

CREATE TABLE varcategories (
    var_id VARCHAR(20) NOT NULL,
    cat_id INTEGER references categorydefs(id),
    PRIMARY KEY(var_id, cat_id) );
        
CREATE TABLE vardefs (
    var_id VARCHAR(20) NOT NULL,
    type VARCHAR(20) NOT NULL,
    labeln INTEGER,
    labelset TEXT,
    datasetname VARCHAR(128),
    collect_form TEXT,
    comment TEXT,
    PRIMARY KEY(var_id) );
'''        

def sql_populate_metadata(metadata):
    '''Generate three SQL tables using hand-coded schema from oai.sql 
    '''
    CATEGORY = 1
    SUBCATEGORY = 2
    
    catdefs_schema = "INSERT INTO categorydefs (type, name) VALUES"
    varcats_schema = "INSERT INTO varcategories (var_id, cat_id) VALUES"
    vardefs_schema = "INSERT INTO vardefs (var_id, type, labeln, labelset, "
    vardefs_schema += "datasetname, collect_form, comment) VALUES"
    
    categories = []
    varcats = []
    vardefs = []
    
    #
    # 1. Create category definitions
    #
    categorydefs = {CATEGORY:{},SUBCATEGORY:{}}
    
    for var_name in metadata:
        categorydefs[CATEGORY].update( {x:1 for x in metadata[var_name]["category"]} )
        categorydefs[SUBCATEGORY].update( {x:1 for x in metadata[var_name]["subcategory"]} )
    
    idx = 1
    category_ids = {CATEGORY:{},SUBCATEGORY:{}}
    for name in sorted(categorydefs[CATEGORY].keys()):
        category_ids[CATEGORY][name] = idx
        categories += ["\t(%s, '%s')" % (CATEGORY, psql_esc_str(name))]
        idx += 1
    for name in sorted(categorydefs[SUBCATEGORY].keys()):
        category_ids[SUBCATEGORY][name] = idx
        categories += ["\t(%s, '%s')" % (SUBCATEGORY, psql_esc_str(name))]
        idx += 1
    #
    # 2. Variable to category mappings
    #
    for var_name in metadata:
        for name in metadata[var_name]["category"]:
            varcats += ["\t('%s', %s)" % (var_name, category_ids[CATEGORY][name])] 
        for name in metadata[var_name]["subcategory"]:
            varcats += ["\t('%s', %s)" % (var_name, category_ids[SUBCATEGORY][name])]
   
    #
    # 3. Vardefs table
    #
    for var_name in metadata:
        
        # escape single quotes
        collect = psql_esc_str(metadata[var_name]["collection"])
        cmmnt = psql_esc_str(metadata[var_name]["comments"])
        ds = psql_esc_str(metadata[var_name]["dataset"])
        dtype = metadata[var_name]["type"]
        label_n = metadata[var_name]["label_n"]
        values = metadata[var_name]["values"]
        
        ds = "'%s'" % ds
        values = "'%s'" % values
        collect = "'%s'" % collect
        cmmnt = "NULL" if cmmnt == "None" else "'%s'" % cmmnt
        
        #"var_id, type, labeln, labelset, "
        #"datasetname, collect_form, comment
        vardefs += [(var_name, dtype, label_n, values, ds, collect, cmmnt)]
  
    vardefs = map(lambda x:"\t('%s', '%s', %s, %s, %s, %s, %s)" % x, vardefs)
    
    print metadata_schema
    print "%s\n%s;" % (catdefs_schema, ",\n".join(categories))
    print
    print "%s\n%s;" % (varcats_schema, ",\n".join(varcats))
    print
    print "%s\n%s;" % (vardefs_schema, ",\n".join(vardefs))

def sql_dataset_schema(tbl_name, sasheader, metadata, pkeys):
    '''
    '''
    dtypes = {"MMDDYY":"DATE"}
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
        elif col.type == "string":
            dtype = "TEXT"

        sql += ["\t%s %s%s" % (col.name, dtype, null)]
    
    if pkeys:
        sql += ["PRIMARY KEY(%s)" % ", ".join(pkeys) ]
    
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
        
        label = psql_esc_str(metadata[var]["label"])
        s = "COMMENT ON column %s.%s is '%s';" % (tbl_name,var,label)
        sql += [s]
    
    comment_sql = "\n".join(sql)
    
    sys.stdout.write(table_sql)
    sys.stdout.write("\n\n")
    sys.stdout.write(comment_sql)
    sys.stdout.write("\n\n")

def sql_insert(data,row_max=ROW_INSERT_MAX):
  
    # use SAS header information to get table name and data types
    tbl_name = data.header.properties.name
    dtypes = {col.name:col.type if col.format !="MMDDYY" else "DATE" 
              for col in data.header.parent.columns}
    
    schema = ""
    rows = []
    for i,row in enumerate(data):
        
        if i == 0:
            dtypes = [dtypes[v] for v in row]
            schema = ",".join(row)
            continue
        
        # escape strings (' and \ characters) and add NULL values to row
        row = [v if v != None else "NULL" for v in row]
        row = [v.replace("'","''").replace("\\","\\\\") if type(v) in [str,unicode] else v 
               for v in row]
        # set data type
        row = ["%s" % v if dtypes[i]=="number" or v == "NULL" else "'%s'" % v 
               for i,v in enumerate(row)]
        row =",".join(row)
        rows += [row]
        
        if len(rows) > row_max:
            
            sys.stdout.write("\nINSERT INTO %s (%s) VALUES\n" % (tbl_name,schema))
            rows = map(lambda x:"\t(%s)" % x, rows)
            rows = ",\n".join(rows)
            sys.stdout.write(rows)
            sys.stdout.write(";\n")
            
            rows = []
    
    # dump remaining rows  
    if rows:
        sys.stdout.write("\nINSERT INTO %s (%s) VALUES\n" % (tbl_name,schema))
        rows = map(lambda x:"\t(%s)" % x, rows)
        rows = ",\n".join(rows)
        sys.stdout.write(rows)
        sys.stdout.write(";\n")


    
def main(args):
    
    #
    # 1. Category/sub-category information 
    #
    metatdata = load_metadata("../data/VG_Variable_tables.bz2")
    sql_populate_metadata(metatdata)

    filelist = [x for x in os.listdir(args.inputdir) 
                if os.path.isfile(args.inputdir+x) and ".zip" in x]

    #
    # 2: Create Table Schema
    #    
    primary_key_defs = {}
    primary_key_defs['acceldatabymin'] = ["ID","PAStudyDay","MINSequence"]
    primary_key_defs["acceldatabyday"] = ["ID","PAStudyDay","VDAYSequence"]
    
    # tables with no primary keys
    primary_key_defs["xray"] = []
    primary_key_defs["mif"] = []
    primary_key_defs["mri"] = []
    primary_key_defs["flxr_kneealign_cooke"] = []
    primary_key_defs["kmri_qcart_eckstein"] = []
    primary_key_defs["kmri_sq_blksbml_bicl"] = []
    primary_key_defs["kmri_fnih_sq_moaks_bic"] = []
    primary_key_defs["kxr_fta_duryea"] = []
    primary_key_defs["kxr_qjsw_duryea"] = []
    primary_key_defs["kxr_sq_bu"] = []
    
    for zfname in filelist:
        filename = "%s%s" % (args.inputdir,zfname)
        zf = zipfile.ZipFile(filename, 'r')
        manifest = sorted(zf.namelist())
        sasfiles = [x for x in manifest if "sas7bdat" in x]
        
        for sasdat in sasfiles:
            data = zf.read(sasdat)
            tmpfile = "/tmp/temp.sas7bdat"
            
            # dump SAS to a temporary file
            with open(tmpfile,"wb") as tmp:
                tmp.write(data)
            
            d = sas7bdat.SAS7BDAT(tmpfile)
          
            # get primary key
            dataset = sasdat.split(".")[0]
            dataset,version = dataset[0:len(dataset)-2], dataset[-2:]
            
            #print d.header.properties.name
            
            if not version.isdigit():
                dataset = dataset + version
                version = None
            
            if dataset not in primary_key_defs:
                primary_key_defs[dataset] = ["ID"]
            
            pkeys = map(lambda x: x if x=="ID" else "V"+version+x, 
                        primary_key_defs[dataset])
            
            # SAS metadata (contains label, format)
            tbl_name = sasdat.split(".")[0]
            sql_dataset_schema(tbl_name, d.header, metatdata, pkeys)
            
            # SAS data: create database INSERT statements
            sql_insert(d)

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--inputdir", type=str, 
                        help="data set input directory")
                   
    args = parser.parse_args()

    # argument error, exit
    if not args.inputdir:
        parser.print_help()
        sys.exit()
     
    main(args)
    

