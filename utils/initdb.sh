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

# Download OAI datasets
# python fetch-data.py

# Create database
psql -c 'CREATE DATABASE '$DBNAME';'

# Load table schema and data
psql -d $DBNAME -f oai-data.sql