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
    
    # determine table for fields
    '''
    query = "SELECT var_id,dataset FROM vardefs WHERE var_id in (%s);"
    query = query % ",".join(map(lambda x:"'%s'" % x, dtype["continuous"].keys()))
    cur.execute(query)  
    results = cur.fetchall()
    tables = {var:tbl for var,tbl in results}
    tables = {x:1 for x in tables.values()}.keys()
    '''
    
    # cluster by pain progression
    vars = ["id",'vid'] + dtype["continuous"].keys()
    query = "SELECT %s FROM jointsx ORDER BY id,vid;" % ",".join(vars)
    
    # KOOS and WOMAC pain scores measure the same thing (essentially)
    
    # create a numpy tensor (4x9x4)
    # normalize to 0..1 (and invert KOOS)
    
    
    
    '''
    pain_scores = {}
    for row in results:
        id,row = row[0],row[1:]
        pain_scores[id] = pain_scores.get(id,[]) + [row]
    
    for id in pain_scores:
        d = sorted(pain_scores[id])
        for row in d:
            print row
    '''
    '''
    # Transform our results to a matrix and cluster the resulting points
    # Strip ID and VERSION
    results = [x[2:] for x in results]
    # Replace None with -1
    results = [[v if v != None else -1 for v in x] for x in results]
    
    
    X = np.array(results)
    pca = PCA(n_components=2)
    y  = pca.fit_transform(X)
    print(pca.explained_variance_ratio_) 
    
    plt.scatter(y[:, 0], y[:, 1])
    plt.show()
    sys.exit()
    
    X = np.array(results)
    tsne = TSNE(n_components=2, random_state=0, init='pca')
    y = tsne.fit_transform(X)
    
    plt.scatter(y[:, 0], y[:, 1])
    ax.xaxis.set_major_formatter(NullFormatter())   
    ax.yaxis.set_major_formatter(NullFormatter())
    plt.axis('tight')
    plt.show()
    '''
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--dbname", type=str, help="OAI database name", 
                        default="oai2")                    
    args = parser.parse_args()

    main(args)