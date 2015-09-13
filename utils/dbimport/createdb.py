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


def norm_col_name(s):
    '''Normalize sql column name
    '''
    return re.sub("^([V]+)(\d\d)",r'\1',s).lower()

def group_by_filename(filelist):
    tmp = {}
    for filename in filelist:
        prefix = filename.split("/")[-1].split(".")[0]
        prefix = re.sub("\d*_SAS","",prefix)
        tmp[prefix] = tmp.get(prefix,[]) + [filename]
        tmp[prefix] = sorted(tmp[prefix])
    return tmp


def create_table_schema(name, vardefs, pkeys):
    
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
    
    return sql
    

primary_key_defs = {}
primary_key_defs["Accelerometry"] = []
primary_key_defs["Biomarkers"] = ["id","vid"]
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
        if grp in ["AllClinical","AccelData","Enrollees"]:
            continue
        
        # create tmp directory for 
        tmp_dir = "%s%s/" % (TMP_ROOT,grp)
        if not os.path.exists(tmp_dir):
            os.mkdir(tmp_dir)
        
        bdatfmt = {}
        var_map,var_fmt = {},{}
        
        for i,zipfname in enumerate(filelist[grp]):
            
            filename = "%s%s" % (args.inputdir,zipfname)
            zf = zipfile.ZipFile(filename, 'r')
            manifest = sorted(zf.namelist())
            sasfiles = [x for x in manifest if "sas7bdat" in x]
            
            # if multiple files, then create a table for each  
            if len(sasfiles) > 1:
                continue
            
            # dump SAS to a temporary file
            sasfile = sasfiles[0]
            data = zf.read(sasfile)
            tmpfile = "%s%s.sas7bdat" % (tmp_dir,i)
            with open(tmpfile,"wb") as tmp:
                tmp.write(data)
            
            d = sas7bdat.SAS7BDAT(tmpfile)
            
            # SAS header format
            var_ids = [(col.name,col.format) for col in d.header.parent.columns]
            for var,dtype in var_ids:
                key = norm_col_name(var) if var not in ["ID","VERSION"] else var.lower()
                if key not in var_map:
                    var_map[key] = {}
                    var_fmt[key] = {}
                var_map[key][var] = 1
                var_fmt[key][dtype] = 1
            
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
        schema = create_table_schema(grp,sql_types,pkeys) 
        
        print schema
        print
        

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
   
    #if args.logging:
    #    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    
    main(args)
    

