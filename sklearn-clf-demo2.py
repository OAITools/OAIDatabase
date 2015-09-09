#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
Scikit-learn OAI Linear Regression Demo
---------------------------------------------------

@author: Jason Alan Fries <jfries [at] stanford.edu>

'''
import sys
import argparse
import psycopg2
import operator
import numpy as np
import math
import datetime

from scipy import stats
import statsmodels.api as sm
import matplotlib.pyplot as plt
from statsmodels.distributions.mixture_rvs import mixture_rvs

from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import MinMaxScaler
from sklearn.cross_validation import KFold
from sklearn.cross_validation import train_test_split
from sklearn.cross_validation import cross_val_score
from sklearn.metrics import mean_squared_error,r2_score
from sklearn.learning_curve import learning_curve
from sklearn.metrics import make_scorer
from sklearn.preprocessing import OneHotEncoder

# -------------------------------------------------------------------
# By default psycopg2 converts postgresql decimal/numeric types to 
# Python Decimal objects. This code forces a float type cast instead
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)
# -------------------------------------------------------------------

# SAS base date is 1-1-1960
SAS_BASE_DATE = datetime.datetime(1960, 1, 1, 0, 0)

# Right and/or Left TKA 
rl_tka_sql = "SELECT ID,v99erkfldt,v99elkfldt FROM outcomes99;"
# Days between enrollment and R or L TKA
days_tka_sql = "SELECT ID,V99ERKDAYS,V99ERKDAYS FROM outcomes99;"

def sm_histogram(X,bins=10):
    
    # kernel density estimation
    kde = sm.nonparametric.KDEUnivariate(X)
    kde.fit()
    
    fig = plt.figure(figsize=(12,8))
    ax = fig.add_subplot(111)
    ax.hist(X, bins=bins, normed=True, color='white')
    ax.plot(kde.support, kde.density, lw=2, color='black');
    plt.show()

def get_table_names():
    
    con = psycopg2.connect(database=args.dbname, user='') 
    cur = con.cursor()
    sql = "SELECT DISTINCT(table_name) FROM information_schema.columns"
    sql += " WHERE table_schema='public';"
    cur.execute(sql)
    results = cur.fetchall()
    
    return results
    
def main(args):
    
    np.random.seed(123456)
    con = psycopg2.connect(database=args.dbname, user='') 
    cur = con.cursor()
    
    # ===============================================
    # Simple Binary Classification Example: 
    # ===============================================
    # Q: Will subject will undergo a R or L TKA 
    #    within the next N months?
    horizon = 12
    
    # -----------------------------------------------
    #
    # STEP 1: Data Set 
    #
    # -----------------------------------------------
    
    # Identify our subjects (anyone with a R or L TKA)
    query = "SELECT ID,v99erkfldt,v99elkfldt FROM outcomes99;"
    cur.execute(query)          
    results = cur.fetchall()

    # 342/4552 Subjects: 203 Right, 210 Left, 71 R+L
    subjects = {id:[rtka,ltka] for id,rtka,ltka in results 
                if rtka != None or ltka != None }
    
    # Visit Dates
    visit_defs = {0:0, 1:12, 2:18 ,3:24, 4:30, 5:36, 6:48, 7:60, 8:72, 9:84}
    
    ids = ["'%s'" % id for id in subjects]
    query = "SELECT ID,V99ELKVSPR,V99ELKVSAF,V99ERKVSPR,V99ERKVSAF "
    query += "FROM outcomes99 WHERE ID in (%s);"
    query = query % ",".join(ids)
    cur.execute(query) 
    results = cur.fetchall()
    
    # ID: Left-Before, Left-After, Right-Before, Right-After
    before_after_dates = {x[0]:x[1:] for x in results}
    min_before_dates = {}
    for id in before_after_dates:
        v1 = before_after_dates[id][0] 
        v2 = before_after_dates[id][2]
        v1 = v1 if v1 != None else 999
        v2 = v2 if v2 != None else 999
        # remove subjects who had a TKA at baseline 
        # but (at present) no TKA for the other knee
        if (v1 == 0 and v2 == 999) or (v1 == 999 and v2 == 0):
            continue
        min_before_dates[id] = min(v1,v2)
    
    
    
    print min_before_dates
    sys.exit()
    #
    # Build Subject Features
    #
    
    
    
    # Select all features from the JointSx data set
    query = """
        SELECT column_name,vardefs.type
        FROM information_schema.columns, vardefs
        WHERE table_schema='public' AND table_name SIMILAR TO '%jointsx%'
        AND column_name=lower(vardefs.var_id);
    """
    cur.execute(query) 
    results = cur.fetchall()
    joint_vars = {x[0]:x[1] for x in results}
 
    # Nominal/Ordinal variables are more tricky. Some OAI fields
    # are clearly just category variables. V03BKRGRCV for example
    # is a categorical variable with the levels:
    #    0: No pain
    #    1: Today
    #    2: Past
    #    .A: Not Expected
    # 
    # The field P01PMRKRCV on the other hand (a 0-10 pain scale) 
    # is ordinal. We could treat P01PMRKRCV as a continuous variable
    # which implicitly assumes the intervals between values are
    # equally spaced. For a pain scale this is a plausible stance,
    # esp. since we have 11 levels. But for the variable 
    # V00KSXRKN3 (Right knee symptoms: knee catch or hang up when moving, last 7 days)
    # which has 6 levels (Never,Rarely,Sometimes,Often,Always,NULL) it is
    # more difficult to know if the intervals are the same. 
    # 
    # Basically you have to make some judgement calls. WOMAC KOOS and pain 
    # scales seem reasonable to represent on continuous scales. Other fields
    # should probably be nominal variables. 

    c_jnt_vars = [var_id for var_id in joint_vars if joint_vars[var_id]=="Continuous"]
    tbls = [x[0] for x in get_table_names()]
    tbls = sorted([x for x in tbls if "jointsx" in x])
    
    ftrs = [(x[0:3],x) for x in c_jnt_vars if x]
    ftrs_by_visit = {}
    for var_id,value in ftrs:
        ftrs_by_visit[var_id] = ftrs_by_visit.get(var_id,[]) + [value]
        
    for var_id in sorted(ftrs_by_visit.keys()):
        print var_id, ftrs_by_visit[var_id]
    
    
    
    
    
    
    # logisitic regression
    
    
    
    sys.exit()
    
    query = """SELECT DISTINCT(table_name) 
            FROM information_schema.columns
            WHERE table_schema='public'"""
    tables = []
    
    # TKA Closest pre/post OAI vists (Right and Left knees)
    vars = ["V99ERKVSPR","V99ERKVSAF","V99ELKVSPR","V99ELKVSAF"]
    
    query = "SELECT var_id,type,labeln FROM vardefs WHERE var_id IN"
    query += "(%s);" % ",".join(map(lambda x:"'%s'" % x, vars))
    cur.execute(query)          
    vardefs = cur.fetchall()
    
    query = "SELECT V99ERKVSPR,V99ERKVSAF,V99ELKVSPR,V99ELKVSAF FROM outcomes99;"
    cur.execute(query)          
    results = cur.fetchall()
    X = np.array(results)
    
    col = {v:1 for v in X[...,0]}
    print col.keys()
    
    # For nominal categories, we convert the to 1-of-n encoding
    # This discards potentially useful information about ordering. 
    onehot = OneHotEncoder()
    #onehot.fit(results)
    # Fix a random seed so that our random number generation is deterministic
    
    
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--dbname", type=str, help="OAI database name", 
                        default="oai")                    
    args = parser.parse_args()

    main(args)