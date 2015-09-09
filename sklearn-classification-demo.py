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

# By default psycopg2 converts postgresql decimal/numeric types to 
# Python Decimal objects. This function forces a float type cast instead
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)

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

def main(args):
    
    np.random.seed(123456)
    con = psycopg2.connect(database=args.dbname, user='') 
    cur = con.cursor()
    
    # Simple Binary Classification Example: 
    # -----------------------------------------------
    # Q: Will subject will undergo a R or L TKA 
    #    within the next N months?
    horizon = 12
    
    # Identify our subjects (anyone with a R or L TKA)
    query = "SELECT ID,v99erkfldt,v99elkfldt FROM outcomes99;"
    cur.execute(query)          
    results = cur.fetchall()

    # 342/4552 Subjects: 203 Right, 210 Left, 71 R+L
    subjects = {id:[rtka,ltka] for id,rtka,ltka in results 
                if rtka != None or ltka != None }
    
    '''
    # distribution of enrollment to TKA (in days)
    ids = ["'%s'" % id for id in subjects]
    query = "SELECT ID,V99ERKDAYS,V99ELKDAYS FROM outcomes99 WHERE ID in (%s);"
    query = query % ",".join(ids)
    cur.execute(query) 
    results = cur.fetchall()
    
    X = reduce(operator.add,[x[1:] for x in results])
    X = [x for x in X if x != None]
    sm_histogram(X)
    '''
    
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
    
    # 320 vs. 4232
    
        
    sys.exit()
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