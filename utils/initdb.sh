#!/bin/sh
# 
# Create OAI Database (OSX/Linux)
#
# @author	Jason Alan Fries 
# @email 	jason-fries [at] stanford [dot] edu
# 
# USAGE: initdb.sh
# OPTIONS: -d XXX
#

DBNAME="oai";

# Create database
psql -c 'CREATE DATABASE '$DBNAME';'

# Load table schema
psql -d $DBNAME -f oai.sql