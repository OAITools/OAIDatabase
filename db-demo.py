#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
PostgreSQL OAI Database Demo
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
from sklearn.cross_validation import train_test_split
from sklearn.cross_validation import cross_val_score
from sklearn.metrics import mean_squared_error,r2_score
from sklearn.learning_curve import learning_curve

# By default psycopg2 converts postgresql decimal/numeric types to 
# Python Decimal objects. This function forces a float type cast instead
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
    
    # Let's model all 
    # model WOMAC Pain (V00WOMKPL) 
    # x: WOMAC is on a scale of 0..20 where 0 indicates no difficulty
    query = """SELECT V00WOMKPL,V00KOOSKPL FROM allclinical00 
    WHERE V00WOMKPL IS NOT NULL AND V00KOOSKPL IS NOT NULL;"""
    cur.execute(query) 
    results = cur.fetchall()
    
    # Let's plot our test data and the corresponding regression fit
    plt.scatter(X_test, y_test, color='black')
    plt.plot(X_test, y_pred, color='blue',linewidth=2)
    plt.show()
    
    # ..and let's plot our learning curve
    train_sizes, train_scores, valid_scores = learning_curve(model, X, y, train_sizes=range(10,1000,10), cv=5)
    
    
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--dbname", type=str, help="OAI database name", 
                        default="oai")                    
    args = parser.parse_args()

    main(args)