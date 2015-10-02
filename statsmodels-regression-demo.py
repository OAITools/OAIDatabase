#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
OAI Database Demo
---------------------------------------------------

**** WORK IN PROGRESS **** 

@author: Jason Alan Fries <jfries [at] stanford.edu>

'''
import sys
import argparse
import psycopg2

import numpy as np
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

# By default psycopg2 converts postgresql decimal/numeric types 
# to Python Decimal objects. This function forces a float type cast instead
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)

def main(args):
    
    np.random.seed(123456)
    
    #
    # Select column names from specific table
    #
    query = """
            SELECT V99ERKDAYS
            FROM outcomes99
            WHERE V99ERKDAYS IS NOT NULL;
            """
    con = psycopg2.connect(database=args.dbname, user='') 
    cur = con.cursor()
    cur.execute(query)          
    results = cur.fetchall()
    results = [x[0] for x in results]
    
    # kernel density estimation
    kde = sm.nonparametric.KDEUnivariate(results)
    kde.fit()
    
    fig = plt.figure(figsize=(12,8))
    ax = fig.add_subplot(111)
    ax.hist(results, bins=10, normed=True, color='red')
    ax.plot(kde.support, kde.density, lw=2, color='black');
    plt.show()
    
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--dbname", type=str, help="OAI database name", 
                        default="oai")                    
    args = parser.parse_args()

    main(args)