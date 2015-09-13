import argparse
import os
import re
import sys
import bz2

PDF2TEXT = "/usr/local/bin/pdftotext"
TMP_ROOT = "/tmp/"

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
    dataset VARCHAR(128),
    collect_form TEXT,
    comment TEXT,
    PRIMARY KEY(var_id) );
'''    

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
        
        vardefs += [(var_name.lower(), dtype, label_n, values, ds, collect, cmmnt)]
  
    vardefs = map(lambda x:"\t('%s', '%s', %s, %s, %s, %s, %s)" % x, vardefs)
    
    print metadata_schema 
    print ("%s\n%s;" % (catdefs_schema, ",\n".join(categories))).lower()
    print
    print ("%s\n%s;" % (varcats_schema, ",\n".join(varcats))).lower()
    print
    print ("%s\n%s;" % (vardefs_schema, ",\n".join(vardefs))).lower()
    

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
  
  
def main(args):
    
    #
    # 1. Category/sub-category information 
    #
    metatdata = load_metadata("../../data/VG_Variable_tables.bz2")
    sql_populate_metadata(metatdata)
        
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--input", type=str, 
                        help="metadata input file")
           
    args = parser.parse_args()

    # argument error, exit
    #if not args.input:
    #    parser.print_help()
    #   sys.exit()
   

    main(args)

