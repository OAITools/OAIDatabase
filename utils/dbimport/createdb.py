#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
'''
import os
import re
import zipfile
import sas7bdat
import argparse
import sys
import operator

TMP_ROOT = "/tmp/"
ROW_INSERT_MAX = 5000

def psql_esc_str(s):
    return s.replace("'","''").replace("\\","\\\\")

def norm_col_name(s):
    '''Normalize sql column name
    '''
    return re.sub("^([V]+)(\d\d)",r'\1',s).lower()

def longest_common_substring(s1, s2):
    m = [[0] * (1 + len(s2)) for i in xrange(1 + len(s1))]
    longest, x_longest = 0, 0
    for x in xrange(1, 1 + len(s1)):
        for y in xrange(1, 1 + len(s2)):
            if s1[x - 1] == s2[y - 1]:
                m[x][y] = m[x - 1][y - 1] + 1
                if m[x][y] > longest:
                    longest = m[x][y]
                    x_longest = x
            else:
                m[x][y] = 0
    return s1[x_longest - longest: x_longest]

def norm_col_label(labels):
    
    labels = [x for x in labels if x] # remove null strs
    labels = reduce(longest_common_substring,labels)
    return labels.strip(":").strip(".").strip()

def group_by_filename(filelist):
    tmp = {}
    for filename in filelist:
        prefix = filename.split("/")[-1].split(".")[0]
        prefix = re.sub("\d*_SAS","",prefix)
        tmp[prefix] = tmp.get(prefix,[]) + [filename]
        tmp[prefix] = sorted(tmp[prefix])
    return tmp

def create_table_schema(name, vardefs, varlabels, pkeys):
    
    sql = "CREATE TABLE %s (\n" % name
    columns = []
    col_names = sorted([x for x in vardefs.keys() 
                        if x not in ["id","vid","version"]])
    first_cols = sorted([x for x in vardefs.keys() if x in ["id","vid","version"]])
    if "version" in first_cols:
        first_cols.remove("version")
        first_cols = first_cols + ["version"]
    col_names = first_cols + col_names
    
    for var in col_names:
        null = " NOT NULL"
        col = "\t%s %s" % (var,vardefs[var])
        if var in pkeys:
            col = col + null
        columns += [col]
    
    if pkeys:
        pkey = "\tPRIMARY KEY(%s)" % (",".join(pkeys))
        columns += [pkey]
        
    sql = sql + "%s);" % ",\n".join(columns)
    
    comments = []
    for var in varlabels:
        label = psql_esc_str(varlabels[var]).decode("utf-8",errors='ignore')
        s = "COMMENT ON column %s.%s is '%s';" % (name,var,label)
        comments += [s]
    
    sql = sql + "\n" + "\n".join(comments) + "\n"
    
    return sql

def sql_insert(name, data, sql_types, vid=None, row_max=ROW_INSERT_MAX):
  
    # use SAS header information to get table name and data types
    tbl_name = data.header.properties.name[:-2]
  
    header = None
    dtypes = None
    rows = []
    for i,row in enumerate(data):
        
        if i == 0:
            # normalize variable names
            header = [norm_col_name(x) if x not in ["id","version"] else x for x in row ]
            dtypes = [sql_types[x] for x in header]
            if vid != None: 
                header = ["vid"] + header
                dtypes = ["NUMERIC"] + dtypes
            continue
        
        if vid != None: 
            row = [vid] + row
            
        # escape strings (' and \ characters) and add NULL values to row
        row = [v if v != None and v != "" else "NULL" for v in row]
        
        row = [psql_esc_str(v) if type(v) in [str,unicode] else v 
               for v in row]
        
        # set data type
        row = ["%s" % v if type(v) in [float,int] or v == "NULL" else "'%s'" % v 
               for v in row]
            
        rows += [",".join(row)]
        
    print "INSERT INTO %s (%s) VALUES\n" % (name,",".join(header))
    rows = map(lambda x:"\t(%s)" % x, rows)
    rows = ",\n".join(rows)
    print "%s;\n\n" % rows
   
def enrollees_sql_insert(name, data, sql_types):
    '''Enrollees data is collapsed into 1 data set (rather than split by visit).
    This creates up to 8 rows per subjects, uniqiuely identified by ID,VID
    '''
    header,normheader,insert_header = None,None,None

    rows = []
    
    for i,row in enumerate(data):
        
        if i == 0:
            # normalize variable names
            header = row
            normheader = [norm_col_name(x) if x not in ["id","version"] else x for x in row]
            tmp = sorted({x:1 for x in normheader if x not in ["id","version"]}.keys())
            insert_header = ["id","vid","version"] + tmp
            
            continue
  
        fields = dict(zip(map(lambda x:x.lower(),header),row))
        vids = map(lambda x:int(x) if x.isdigit() else 0,[x[1:3] for x in header if x[0]!="v"])
        vids = {v:1 for v in vids}
        
        for vid in vids:
            subrow = []
            vidprefix = "V" + ("0" + str(vid))[-2:]
        
            
            for var in insert_header:
              
                unvar = (vidprefix + var[1:]).lower()
                
                if var in fields:
                  
                    subrow += [fields[var]]
                elif unvar in fields:
            
                    subrow += [fields[unvar]]
                else:
                    subrow += [None]
            
            subrow[1] = vid
            rows += [subrow]
        
        
    # normalize sql statements
    for i,row in enumerate(rows):
        
        # escape strings (' and \ characters) and add NULL values to row
        row = [v if v != None and v != "" else "NULL" for v in row]
        
        row = [psql_esc_str(v) if type(v) in [str,unicode] else v 
               for v in row]
        
        # set data type
        row = ["%s" % v if type(v) in [float,int] or v == "NULL" else "'%s'" % v 
               for v in row]
        
        rows[i] = ",".join(row)
 
    print "INSERT INTO %s (%s) VALUES\n" % (name,",".join(insert_header))
    rows = map(lambda x:"\t(%s)" % x, rows)
    rows = ",\n".join(rows)
    print "%s;\n\n" % rows


primary_key_defs = {}
primary_key_defs["Accelerometry"] = ["id"]
primary_key_defs["Biomarkers"] = ["id","vid"]
primary_key_defs["Enrollees"] = ["id","vid"]
primary_key_defs["JointSx"] = ["id","vid"]
primary_key_defs["MIF"] = []
primary_key_defs["MRI"] = []
primary_key_defs["MedHist"] = ["id","vid"]
primary_key_defs["Nutrition"] = ["id","vid"]
primary_key_defs["Outcomes"] = ["id"]
primary_key_defs["PhysExam"] = ["id","vid"]
primary_key_defs["SubjectChar"] = ["id","vid"]
primary_key_defs["Xray"] = []
primary_key_defs["flXR_KneeAlign_Cooke"] = []
primary_key_defs["kMRI_QCart_Eckstein"] = []
primary_key_defs["kMRI_QCart_Link"] = []
primary_key_defs["kMRI_QCart_VS"] = []
primary_key_defs["kMRI_SQ_BICL"] = []
primary_key_defs["kMRI_SQ_BLKSBML_BICL"] = []
primary_key_defs["kMRI_SQ_MOAKS_BICL"] = []
primary_key_defs["kMRI_SQ_WORMS_Link"] = []
primary_key_defs["kXR_FTA_Duryea"] = []
primary_key_defs["kXR_QJSW_Duryea"] = []
primary_key_defs["kXR_QJSW_Rel_Duryea"] = []
primary_key_defs["kXR_SQ_BU"] = []
primary_key_defs["kXR_SQ_Rel_BU"] = []


def main(args):
    
    filelist = [x for x in os.listdir(args.inputdir) 
                if os.path.isfile(args.inputdir+x) and ".zip" in x]
    
    filelist = group_by_filename(filelist)
  
    for grp in filelist:
        
        # skip these data sets as they require special handing
        if grp in ["AllClinical","AccelData"]:
            continue
        
        # create tmp directory for 
        tmp_dir = "%s%s/" % (TMP_ROOT,grp)
        if not os.path.exists(tmp_dir):
            os.mkdir(tmp_dir)
        
        bdatfmt = {}
        var_map,var_fmt,var_labels = {},{},{}
        
        for i,zipfname in enumerate(filelist[grp]):
            
            filename = "%s%s" % (args.inputdir,zipfname)
            zf = zipfile.ZipFile(filename, 'r')
            manifest = sorted(zf.namelist())
            sasfiles = [x for x in manifest if "sas7bdat" in x]

            if len(sasfiles) > 1:
                continue
            
            # dump SAS to a temporary file
            data = zf.read(sasfiles[0])
            tmpfile = "%s%s.sas7bdat" % (tmp_dir,i)
            with open(tmpfile,"wb") as tmp:
                tmp.write(data)
            
            d = sas7bdat.SAS7BDAT(tmpfile)
            
            # SAS header format
            var_ids = [(col.name,col.format,col.label) for col in d.header.parent.columns]
            for var,dtype,label in var_ids:
                key = norm_col_name(var) if var not in ["ID","VERSION"] else var.lower()
                if key not in var_map:
                    var_map[key] = {}
                    var_fmt[key] = {}
                    var_labels[key] = []
                var_map[key][var] = 1
                var_fmt[key][dtype] = 1
                var_labels[key] += [label]
                
            # types actually created by sas2bdat
            header = [norm_col_name(col.name) for col in d.header.parent.columns]
            for idx,row in enumerate(d):
                if idx == 0:
                    continue
                
                for j in range(0,len(row)):
                    t = type(row[j])
                    if header[j] not in bdatfmt:
                        bdatfmt[header[j]] = {}
                    bdatfmt[header[j]][t] = bdatfmt[header[j]].get(t,0) + 1
            
        # normalize labels
        for var in var_labels:
            var_labels[var] = norm_col_label(var_labels[var])
                
                
        var_fmt["id"] = var_fmt["version"] = {"$":1}
        
        # assign majority type as column data type
        for var in bdatfmt:
            types = sorted(bdatfmt[var].items(),key=operator.itemgetter(1),reverse=1)
            bdatfmt[var] = types[0][0] 
            
        # confirm consistent data types across all fields
        # use this as a data type in SQL schema
        sql_types = {}
        for var in var_fmt:
            if var not in bdatfmt:
                sys.stderr.write("FATAL ERROR -- exiting")
                sys.stderr.write("%s %s" % (grp, var))
                sys.exit()
          
            sql_types[var] = "TEXT" if bdatfmt[var] in [unicode,str] else "NUMERIC"
            
            if "MMDDYY" in var_fmt[var]:
                sql_types[var] = "DATE"
            
        # manually add visit column to tables containing
        # multiple visits
        if grp not in ["Outcomes"]:
            sql_types["vid"] = "INTEGER"
        
        pkeys = primary_key_defs[grp] if grp in primary_key_defs else []
        schema = create_table_schema(grp,sql_types,var_labels,pkeys) 
        
        print schema
        print
        
        if grp == "Enrollees":
            tmpfile = "%s%s.sas7bdat" % (tmp_dir,0)
            data = sas7bdat.SAS7BDAT(tmpfile)
            enrollees_sql_insert(grp, data, sql_types)
            continue
        
        # only 1 file (don't use VID)
        if len(filelist[grp]) == 1:
            tmpfile = "%s%s.sas7bdat" % (tmp_dir,0)
            data = sas7bdat.SAS7BDAT(tmpfile)
            sql_insert(grp, data, sql_types, vid=None)
            continue
        
        for i in range(0,len(filelist[grp])):
            tmpfile = "%s%s.sas7bdat" % (tmp_dir,i)
            data = sas7bdat.SAS7BDAT(tmpfile)
            sql_insert(grp, data, sql_types, vid=i)
                       
        

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--inputdir", type=str, 
                        help="data set input directory")
    parser.add_argument("-m","--no-metadata", action='store_false', dest="metadata",
                        help="output metadata schema")   
    parser.add_argument("-l","--no-logging", action='store_false', dest="logging",
                        help="disable logging")            
    args = parser.parse_args()

    # argument error, exit
    if not args.inputdir:
        parser.print_help()
        sys.exit()
   
    main(args)
    

