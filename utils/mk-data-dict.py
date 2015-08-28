#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
Initialize Osteoarthitis Initiative (OAI) Database
---------------------------------------------------

This is pretty tedious. SAS XPORT files (*.xpt files) don't appear to have an 
open source reader implementation, at least not the compressed V8 variety. The 
Python library xport fails to read OAI xpt files, meaning we can't use 
native SAS column information to generate our database schema. 

Instead, we make use of the PDFs provided that describe data format and labels.
We convert PDFs to text, then use a script to generate a data dictionary of
column names, data type, and label. This dictionary is used by init-db.py
to generate our table schema 


@author: Jason Alan Fries <jfries [at] stanford.edu>

'''

def main():
    
    pass