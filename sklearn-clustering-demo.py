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
import math
import matplotlib.pyplot as plt
from sklearn.manifold import TSNE
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib.ticker import NullFormatter
from sklearn import manifold, datasets
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans,SpectralClustering
from preprocessing.imputation import ses, interpolate

# By default psycopg2 converts postgresql decimal/numeric types to 
# Python Decimal objects. This function forces a float type cast instead
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)


def main(args):
    
    np.random.seed(123456)
    con = psycopg2.connect(database=args.dbname, user='') 
    cur = con.cursor()
    
    # Select all features related to WOMAC/KOOS pain subcategories
    ftr_cats = ["womac pain","koos pain"]
    
    query = "SELECT var_id FROM varcategories WHERE varcategories.cat_id IN "
    query += "(SELECT id FROM categorydefs WHERE name in (%s) AND type=2);"
    query = query % ",".join(map(lambda x:"'%s'" % x,ftr_cats))
    cur.execute(query)          
    results = [x[0] for x in cur.fetchall()]
    
    # get variable types (nominal or continuous)
    query = "SELECT var_id,type,labeln FROM vardefs WHERE var_id in (%s);"
    query = query % ",".join(map(lambda x:"'%s'" % x,results))
    cur.execute(query)  
    results = cur.fetchall()
    
    # sort by data type
    dtype = {}
    dtype["nominal"] = {var:labeln for var,t,labeln in results if t == "nominal"}
    dtype["continuous"] = {var:labeln for var,t,labeln in results if t == "continuous"}
    
    print dtype["nominal"]
    print dtype["continuous"]
    sys.exit()
    
    # KOOS and WOMAC pain scores measure similar things (essentially)
    vars = ["id",'vid'] + sorted(dtype["continuous"].keys())
    query = "SELECT %s FROM jointsx ORDER BY id,vid;" % ",".join(vars)
    cur.execute(query)  
    results = cur.fetchall()
    
    # create subjects
    subjects = {}
    for row in results:
        id,row = row[0],row[1:]
        subjects[id] = subjects.get(id,[]) + [row]
        
    # create a numpy tensor of pain data (4796, 10, 4)
    X = []
    MIN_NAN_THRESHOLD  = 0.5 # must contain < 50% null obs (NaN)
    func = np.vectorize(interpolate)
    
    for id in subjects:
        m = np.empty((10,4),dtype=np.float64)
        m.fill(np.nan)
        
        #vid, kooskpl, kooskpr, womkpl, womkpr = row
        for row in subjects[id]:
            vid,tmp = row[0],row[1:]
            m[vid] = tmp
            
        n = np.count_nonzero(~np.isnan(m))
        if n/40.0 < MIN_NAN_THRESHOLD:
            continue
        
        # interpolate missing values
        interpolate(m)
        X += [m]
           
    X = np.array(X)
    
    # invert KOOS scale
    # 0 = no problems  1 = extreme problems
    X[...,...,0:2] = 100 - X[...,...,0:2]
    
    # Standardize to 0..1 range
    X[...,...,0:2] /= 100
    X[...,...,2:] /= 20
    
    # for each subject, create a pain progression time series
    # calculated as the sum of R+L KOOS/WOMAC scores
    pain = [] #np.empty((X.shape[0],X.shape[1]))
    for i in range(0,X.shape[0]):
        koos =  X[i][...,0] + X[i][...,1]
        womac = X[i][...,2] + X[i][...,3]
        x = (koos + womac) / 2.0
        
        # HACK: remove any rows containing a NaN (for now)
        if np.count_nonzero(~np.isnan(x)) != 10:
            continue
            # replace NaN with the mean for this row
            x[np.isnan(x)] = np.nanmean(x)
         
        pain += [x]
        
    pain = np.array(pain)  
 
    
 
    # cluster using SpectralClustering
    k = 8
    clstr = SpectralClustering(n_clusters=k) 
    clstr.fit(pain)   
       
    for i in range(0,k):
        
        axes = plt.gca()
        axes.set_ylim([0.0,2.0])
        
        for j in range(0,pain.shape[0]):
            if clstr.labels_[j] != i:
                continue
            plt.plot(range(0,10), pain[j], color='blue', linewidth=0.1)
        
        plt.savefig("/users/fries/desktop/kmeans/%s.pdf" % i)
        plt.clf()
    
     
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--dbname", type=str, help="OAI database name", 
                        default="oai2")                    
    args = parser.parse_args()

    main(args)