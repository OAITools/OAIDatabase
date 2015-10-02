#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
Scikit-learn OAI FeatureBuilder Demo
---------------------------------------------------

NOTE: The FeatureBuilder class is a total hack and could
be done much better


TODO: Need to write unit tests to confirm all our data
is properly transformed!!!

@author: Jason Alan Fries <jfries [at] stanford.edu>

'''
import sys
from datasets import FeatureBuilder
from datasets.oai import *

print_oai_categories()

# The FeatureBuilder enforces the constraint that rows
# will always be sorted by subject id in ascending order.
# Features are returned as numpy multidimensional arrays (tensors)
ftrbldr = FeatureBuilder()
print("Subject N:%s\n" % len(ftrbldr.row_names))

###############################################################################

# Example 1: Continuous features
x1 = ftrbldr.get_feature("jointsx","vwomkpr")
print("%s" % get_var_description("jointsx","vwomkpr"))
print(x1.shape)
print x1[0,...,...].flatten()

# Example 2: Nominal features (one-hot encoding)
x2 = ftrbldr.get_feature("jointsx","vwplkn5",force_continuous=False)
print("%s" % get_var_description("jointsx","vwplkn5"))
print(x2.shape)
print x2[0,...,...]

# Example 2: Nominal features forced as continuous
x2 = ftrbldr.get_feature("jointsx","vwplkn5",force_continuous=True)
print("%s" % get_var_description("jointsx","vwplkn5"))
print(x2.shape)
print x2[0,...,...].flatten()

###############################################################################

# We can also fetch variable categories. 
# get_category_vars returns a dictionary of all continuous and nominal vars
# in the provided categories. The dict contains var field names, table, and
# label domain size (0 for continuous)

ftr_cats = ["womac pain","koos pain"]
ftr_names = get_category_vars(ftr_cats)
print(ftr_names['continuous'])
print(ftr_names['nominal'])
print("\n")

ftr_cats = ["demographics"]
ftr_names = get_category_vars(ftr_cats)
print(ftr_names['continuous'])
print(ftr_names['nominal'])
print("\n")

ftr_cats = ["physical activity","strength measures","health care access"]
ftr_names = get_category_vars(ftr_cats)
print(ftr_names['continuous'])
print(ftr_names['nominal'])
print("\n")
