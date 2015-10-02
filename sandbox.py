#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
Scikit-learn OAI FeatureBuilder Demo
---------------------------------------------------

TODO: Need to write unit tests to confirm all our data
is properly transformed!!!

@author: Jason Alan Fries <jfries [at] stanford.edu>

'''
from datasets import FeatureBuilder
from datasets.oai import print_oai_categories, get_var_description

#print_oai_categories()

# The FeatureBuilder enforces the constraint that rows
# will always be sorted by subject id in ascending order
ftrbldr = FeatureBuilder()
print("Subject N:%s\n" % len(ftrbldr.row_names))

# continuous feature
x1 = ftrbldr.get_feature("jointsx","vwomkpr")
print("%s\n" % get_var_description("jointsx","vwomkpr"))
print(x1.shape)

# nominal feature
x2 = ftrbldr.get_feature("jointsx","vwplkn5")
print("%s\n" % get_var_description("jointsx","vwplkn5"))