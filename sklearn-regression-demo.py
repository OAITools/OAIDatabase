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
    
    #
    # scikit-learn
    #
    
    # Let's model a simple linear relationship using OAI data:
    # model WOMAC Pain (V00WOMKPL) as a function of KOOS Pain (V00KOOSKPL)
    # x: KOOS is on a scale of 0..100 where 0 indicates extreme problems
    # y: WOMAC is on a scale of 0..20 where 0 indicates no difficulty
    #
    # NOTE: we are removing instances where one or both observations are missing
    # In a real modeling problem we have to be more mindful of missing values. 
    query = """SELECT V00WOMKPL,V00KOOSKPL FROM allclinical00 
    WHERE V00WOMKPL IS NOT NULL AND V00KOOSKPL IS NOT NULL;"""
    cur.execute(query) 
    results = cur.fetchall()
    
    # Fix a random seed so that our random number generation is deterministic
    np.random.seed(123456)
    
    # Pull out a random choice of 25% of data to use as a final test set;
    # we will *not* use this in any way for training our model. For 
    # simple linear regression we don't really have any hyperparameters to 
    # tune, so we won't create a validation set. Generally, Hastie et al.   
    # suggest Training (50%) Validation (25%) Training (25%) 
    train,test = train_test_split(results, test_size=0.25)
     
    # * the asterisk operator (called the "splat" or "positional expansion" 
    # operator) expands tuples [(1,2),(3,4)] becomes [1,3] and [2,4]
    y,X = zip(*train)
    X = np.array(X).reshape(-1,1)
    y = np.array(y).reshape(-1,1)
    
    # Evaluate our model using 5-fold cross validation
    model = LinearRegression()
    kf = KFold(X.shape[0], n_folds=5) # k-Fold cross-validation iterator. 
    
    # We can use several different scoring functions here:
    # r2 (coeffecient of determination), mean absolute error
    scores = cross_val_score(model,X,y,cv=kf,scoring="mean_squared_error")
    print("Mean Training Set Error (MSE): %.2f" % np.mean(scores))
    
    # Now fit on *all* our training data and then see how 
    # well we predict test set data
    model.fit(X,y)
    y_test,X_test = zip(*test)
    X_test = np.array(X_test).reshape(-1,1)
    y_test = np.array(y_test).reshape(-1,1)
    y_pred = model.predict(X_test)
    
    print("Test Set Error: %.2f" % mean_squared_error(y_test,y_pred))
    print(r2_score(y_test,y_pred))
    
    # Let's plot our test data and the corresponding regression fit
    plt.scatter(X_test, y_test, color='black')
    plt.plot(X_test, y_pred, color='blue', linewidth=2)
    plt.show()
    
    # ..and let's plot our learning curve
    train_sizes, train_scores, valid_scores = learning_curve(model, X, y, train_sizes=range(10,1000,10), cv=5)
    
    
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--dbname", type=str, help="OAI database name", 
                        default="oai")                    
    args = parser.parse_args()

    main(args)