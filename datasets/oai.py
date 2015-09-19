import psycopg2

# -------------------------------------------------------------------
# By default psycopg2 converts postgresql decimal/numeric types to 
# Python Decimal objects. This code forces a float type cast instead
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)
# -------------------------------------------------------------------

def get_table_names(dbname):
    
    con = psycopg2.connect(database=dbname, user='') 
    cur = con.cursor()
    sql = "SELECT DISTINCT(table_name) FROM information_schema.columns"
    sql += " WHERE table_schema='public';"
    cur.execute(sql)
    results = cur.fetchall()
    
    return results








class FeatureBuilder(object):
    '''
    '''
    def __init__(self,dbname,table):
        self.dbname = dbname
        self.con = psycopg2.connect(database=dbname, user='') 
        self.cur = self.con.cursor()
        self.table_names = get_table_names(dbname)
        
    def get_feature(self,subject_id,var_id):
        pass
    
    
    def batch_get_feature(self,subject_ids,var_id):
        pass