import psycopg2
import numpy as np
from sklearn.preprocessing import OneHotEncoder

DBNAME = "oai2"

# -------------------------------------------------------------------
# By default psycopg2 converts postgresql decimal/numeric types to 
# Python Decimal objects. This code forces a float type cast instead
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)
# -------------------------------------------------------------------

def get_table_names(dbname=DBNAME):
    
    con = psycopg2.connect(database=DBNAME, user='') 
    cur = con.cursor()
    sql = "SELECT DISTINCT(table_name) FROM information_schema.columns"
    sql += " WHERE table_schema='public';"
    cur.execute(sql)
    results = cur.fetchall()
    cur.close()
    
    return results


def get_category_vars(ftr_cats):
    '''
    '''
    con = psycopg2.connect(database=DBNAME, user='') 
    cur = con.cursor()
    
    query = """SELECT DISTINCT var_id FROM varcategories
            WHERE varcategories.cat_id 
            IN (SELECT id FROM categorydefs WHERE name in (%s));"""
    
    query = query % ",".join(map(lambda x:"'%s'" % x,ftr_cats))
    cur.execute(query)          
    results = [x[0] for x in cur.fetchall()]
    
    # get variable types (nominal or continuous)
    query = "SELECT var_id,type,labeln,dataset FROM vardefs WHERE var_id in (%s);"
    query = query % ",".join(map(lambda x:"'%s'" % x,results))
    cur.execute(query)  
    results = cur.fetchall()
    
    # sort by data type
    dtype = {}
    dtype["nominal"] = {var:(labeln,dataset) for var,t,labeln,dataset 
                        in results if t == "nominal"}
    dtype["continuous"] = {var:(labeln,dataset) for var,t,labeln,dataset 
                           in results if t == "continuous"}
    
    return dtype
    

def get_var_description(table_name,var_id):
    '''Ugly SQL for fetching table var comments. Found on
    http://www.postgresonline.com/journal/archives/215-Querying-table,-view,-column-and-function-descriptions.html

    '''
    query = """SELECT a.attname As column_name,  d.description
   FROM pg_class As c
    INNER JOIN pg_attribute As a ON c.oid = a.attrelid
   LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
   LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
   LEFT JOIN pg_description As d ON (d.objoid = c.oid AND d.objsubid = a.attnum)
   WHERE a.attname='%s' AND n.nspname = 'public' AND c.relname = '%s'
   ORDER BY n.nspname, c.relname, a.attname;
    """
    con = psycopg2.connect(database=DBNAME, user='') 
    cur = con.cursor()
    query = query % (var_id,table_name)
    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    
    return "[%s] %s" % results[0]

def print_oai_categories():
    '''Every variable is assigned 1 or more category and subcategory labels
    '''
    con = psycopg2.connect(database=DBNAME, user='') 
    cur = con.cursor()
    cur.execute("SELECT type,name FROM categorydefs;")
    results = sorted(cur.fetchall())
    
    # print categories
    print("CATEGORIES\n------------------------")
    for name in [x for x in results if x[0]==1]:
        print("  %s" % name[1])
    
    # print subcategortes
    print("\nSUBCATEGORIES\n------------------------")
    for name in [x for x in results if x[0]==2]:
        print("  %s" % name[1])
            
    cur.close()


class FeatureBuilder(object):
    ''' Return a tensor of features, given a table and set of var_ids
    4796 x 10 x (number of features) 
    Nominal features are automatically converted to one-hot representations
    
    TODO: This could be done much more effeciently
    '''
    def __init__(self,dbname=DBNAME):
        self.dbname = dbname
        self.con = psycopg2.connect(database=dbname, user='') 
        self.cur = self.con.cursor()
        self.table_names = get_table_names(dbname)
        
        # create row_id -> subject_id mapping
        self.cur.execute("SELECT DISTINCT(id) FROM jointsx;")
        results = sorted([int(x[0]) for x in self.cur.fetchall()])
        self.row_names = map(str,results)
    
    def get_feature(self,table,var_id,force_continuous=False):
        
        # get variable type (nominal or continuous)
        query = "SELECT var_id,type,labeln,labelset FROM vardefs WHERE var_id in (%s);"
        query = query % ",".join(map(lambda x:"'%s'" % x,[var_id]))
        self.cur.execute(query)
        results = self.cur.fetchall()
        
        assert len(results) > 0  

        # query data
        _,var_type,label_n,labelset = results[0]
        vars = ["id",'vid'] + [var_id]
        query = "SELECT %s FROM jointsx ORDER BY id,vid;" % ",".join(vars)
        self.cur.execute(query)  
        results = self.cur.fetchall()
        
        subjects = {}
        for row in results:
            sid,row = row[0],row[1:]
            subjects[sid] = subjects.get(sid,[]) + [row]
        
        #
        # CASE 1: continuous variable
        #
        X = []
        if var_type == 'continuous' or force_continuous:
            
            for sid in self.row_names:
                m = np.empty((10,1),dtype=np.float64)
                m.fill(np.nan)
            
                for row in subjects[sid]:
                    vid,tmp = row[0],row[1:]
                    m[vid] = tmp
            
                X += [m]
            
            X = np.array(X)
            
        #
        # CASE 1: nominal variable
        #
        else:
            # category variable domain
            domain = [int(x) for x in labelset.split("|") if x!= "none"]
            null_id = max(domain) + 1
            enc = OneHotEncoder(n_values=null_id+1)
            
            for sid in self.row_names:
                m = np.empty((10,1),dtype=np.int8)
                # fill out empty matrix with the default label for None
                m.fill(null_id)
                
                for row in subjects[sid]:
                    vid,tmp = row[0],row[1:]
                    # replace None with null_id value
                    tmp = [null_id if x==None else int(x) for x in tmp]
                    m[vid] = tmp
            
                X += [m]
            
            X = np.array(X)
            
            
            Xt = []
            for j in range(0,X.shape[1]):
                x = X[...,j,...]
                x = enc.fit_transform(x).toarray()
                Xt += [x]
               
            X = np.concatenate(Xt).reshape((4796, 10, -1),order='F').astype(np.int8)
            
        '''
        print X.shape
        for i in range(0,10):
            test = X[i,...,...]
            print self.row_names[i],test.shape
            print test
        '''
            
            
            
        return X
        
    
    
    
    
'''
class TableFeatureBuilder(FeatureBuilder):
    
    def __init__(self):
        super(FeatureBuilder, self).__init__()
        
    def get_features(self,categories):
        pass
    
    
class CategoryFeatureBuilder(FeatureBuilder):
    
    def __init__(self):
        super(FeatureBuilder, self).__init__()
        
    def get_category_features(self,categories):
        pass
'''        