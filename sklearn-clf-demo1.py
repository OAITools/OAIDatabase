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



def main(args):
    
    np.random.seed(123456)
    
    # -----------------------------------------------
    # Simple Binary Classification Example
    # -----------------------------------------------
    # Q: Will subject will undergo a R or L TKA 
    #    by their next OAI visit?
 
    # Load Data Set 
    X = datasets.oai.tka_demo
    y = []
    
    
    
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--dbname", type=str, help="OAI database name", 
                        default="oai")                    
    args = parser.parse_args()

    main(args)