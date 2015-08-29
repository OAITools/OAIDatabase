#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
---------------------------------------------------
Initialize Osteoarthitis Initiative (OAI) Database
---------------------------------------------------

Osteoarthitis Initiative data is available 

SAS file reader3
https://pypi.python.org/pypi/sas7bdat


@author: Jason Alan Fries <jason-fries@uiowa.edu>

'''
import glob
import sys, os

import argparse
import logging, pprint
import random
import cPickle as pickle
import numpy as np
import operator
from sas7bdat import SAS7BDAT

logger = logging.getLogger('init-db')

DEBUG = True


import sas7bdat

    
    
def main(args):
    
    # SAS Transport files XPORT
    infile = '/Users/fries/Desktop/AllClinical09_SAS/allclinical09.sas7bdat'
    with SAS7BDAT(infile) as f:
        for row in f:
            print len(row)
            break

    
    pass

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--indir", type=str, 
                        help="corpus input directory")
    parser.add_argument("-o","--outdir", type=str, 
                        help="output directory for rankings")
                        
    args = parser.parse_args()
   
    #if args.logging:
    #    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    
    # argument error, exit
    #if not args.indir or not args.modeldir or not args.outdir:
    #    parser.print_help()
    #    sys.exit()
        
    main(args)
    
