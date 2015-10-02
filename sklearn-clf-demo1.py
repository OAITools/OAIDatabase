#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
Scikit-learn OAI Linear Regression Demo
---------------------------------------------------

**** WORK IN PROGRESS **** 


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

'''
    query = "SELECT var_id,dataset FROM vardefs WHERE var_id in (%s);"
    query = query % ",".join(map(lambda x:"'%s'" % x, dtype["continuous"].keys()))
    cur.execute(query)  
    results = cur.fetchall()
    tables = {var:tbl for var,tbl in results}
    tables = {x:1 for x in tables.values()}.keys()
'''


def main(args):
    
    np.random.seed(123456)
    
    # -----------------------------------------------
    # Simple Binary Classification Example
    # -----------------------------------------------
    # Q: Will subject will undergo a R or L TKA 
    #    by their next OAI visit?
 
    # Identify our subjects (anyone with a R or L TKA)
    con = psycopg2.connect(database="oai2", user='') 
    cur = con.cursor()
    query = "SELECT id,verkfldt,velkfldt FROM outcomes;"
    cur.execute(query)
    results = cur.fetchall()

    # 342/4552 Subjects: 203 Right, 210 Left, 71 R+L
    subjects = {id:[rtka,ltka] for id,rtka,ltka in results 
                if rtka != None or ltka != None }
    print len(subjects)
    # Visit Dates
    visit_defs = {0:0, 1:12, 2:18 ,3:24, 4:30, 5:36, 6:48, 7:60, 8:72, 9:84}
    
    ids = ["'%s'" % id for id in subjects]
    query = "SELECT ID,V99ELKVSPR,V99ELKVSAF,V99ERKVSPR,V99ERKVSAF "
    query += "FROM outcomes99 WHERE ID in (%s);"
    query = query % ",".join(ids)
    cur.execute(query) 
    results = cur.fetchall()
 
 
    # Load Data Set 
    #X = datasets.oai.tka_demo
    #y = []
    
    
    
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--dbname", type=str, help="OAI database name", 
                        default="oai")                    
    args = parser.parse_args()

    main(args)