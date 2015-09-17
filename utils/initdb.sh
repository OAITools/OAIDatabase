#!/bin/sh
# 
# Create OAI Database (OSX/Linux)
#
# @author	Jason Alan Fries 
# @email 	jason-fries [at] stanford [dot] edu
# 
# USAGE: initdb.sh
#

DBNAME="oai";
DATADIR="/tmp/"

# Make temp download directory
mkdir $DATADIR/oai-data/

# Download OAI datasets
python fetch-data.py -o $DATADIR/oai-data/

# Create database
psql -c 'CREATE DATABASE '$DBNAME';'

# Create SQL table schema and data
python dbimport/dbcreate.py -i $DATADIR/oai-data/ > $DATADIR/oai-data.sql

# Load table schema and data
psql -d $DBNAME -f $DATADIR/oai-data.sql
