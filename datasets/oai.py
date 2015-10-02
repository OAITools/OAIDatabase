import psycopg2
import numpy as np

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
        
    
    def get_feature(self,table,var_id):
        
        # get variable type (nominal or continuous)
        query = "SELECT var_id,type,labeln FROM vardefs WHERE var_id in (%s);"
        query = query % ",".join(map(lambda x:"'%s'" % x,[var_id]))
        self.cur.execute(query)
        results = self.cur.fetchall()
        assert len(results) > 0  
       
        _,var_type,label_n = results[0]
        
        # create continuous variable tensor
        if var_type == 'continuous':
            
            vars = ["id",'vid'] + [var_id]
            query = "SELECT %s FROM jointsx ORDER BY id,vid;" % ",".join(vars)
            self.cur.execute(query)  
            results = self.cur.fetchall()
            
            subjects = {}
            for row in results:
                id,row = row[0],row[1:]
                subjects[id] = subjects.get(id,[]) + [row]
            
            X =[]
            for id in subjects:
                m = np.empty((10,1),dtype=np.float64)
                m.fill(np.nan)
            
                for row in subjects[id]:
                    vid,tmp = row[0],row[1:]
                    m[vid] = tmp
            
                X += [m]
            
            return np.array(X)
                
        # nomimal
        else:
            pass
        
        
        
    
    
    
    
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