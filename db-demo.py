#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
OAI Database Demo
---------------------------------------------------

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

# By default psycopg2 converts postgresql decimal/numeric types 
# to Python Decimal objects. This function forces a float type cast instead
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)

def main(args):
    
    #
    # Select column names from specific table
    #
    query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='allclinical00'
            """
    con = psycopg2.connect(database=args.dbname, user='') 
    cur = con.cursor()
    cur.execute(query)          
    results = cur.fetchall()
    
    #
    # scikit-learn
    #
    
    # Let's model a simple linear relationship using OAI data:
    # model WOMAC Pain (V00WOMKPL) as a function of KOOS Pain (V00KOOSKPL)
    # x: KOOS is on a scale of 0..100 where 0 indicates extreme problems
    # y: WOMAC is on a scale of 0..20 where 0 indicates no difficulty
    
    query = """SELECT V00WOMKPL,V00KOOSKPL FROM allclinical00 
    WHERE V00WOMKPL IS NOT NULL AND V00KOOSKPL IS NOT NULL;"""
    cur.execute(query) 
    results = cur.fetchall()
    
    # fix a random seed so that our random number generation is deterministic
    numpy.random.seed(123456)
    
    y,x = zip(*results) 
    x = np.array(x).reshape(-1,1)
    y = np.array(y).reshape(-1,1)
    
    # * the asterisk expands tuples [(1,2),(3,4)] becomes [1,3] and [2,4]
  
    # evaluate our performance using 5-fold cross validation
    model = LinearRegression()
    
    # k-Fold cross-validation iterator. 
    kf = KFold(x.shape[0], n_folds=5)
    for train, test in kf:
        
        
    
    
    sys.exit()
    
    #
    # statsmodels visuzliation demo
    #
    query = "SELECT V00AGE FROM allclinical00;"
    cur.execute(query) 
    age_results = cur.fetchall()
    age_results = [x[0] for x in age_results]
    
    # kernal density estimation
    kde = sm.nonparametric.KDEUnivariate(age_results)
    kde.fit()
    
    fig = plt.figure(figsize=(12,8))
    ax = fig.add_subplot(111)
    ax.hist(age_results, bins=35, normed=True, color='red')
    ax.plot(kde.support, kde.density, lw=2, color='black');
    plt.savefig("/users/fries/test.pdf")
    
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--dbname", type=str, help="OAI database name", 
                        default="oai")                    
    args = parser.parse_args()

    main(args)