#!/bin/sh
# 
# Create OAI Database (OSX/Linux)
# Script assumes file are provided in a folder named OAI under
# the $DATADIR path or downloaded by the script directly.
#
# @author	Jason Alan Fries 
# @email 	jason-fries [at] stanford [dot] edu
# 
# USAGE: initdb.sh
#

DBNAME="oai3";
DATADIR="/tmp/"

if [ "$1" == "-d" ]; then
	# Make temp download directory
	mkdir $DATADIR/OAI/

	# Download OAI datasets
	python fetch-data.py -o $DATADIR/OAI/
fi

# Create database
psql -c 'DROP DATABASE IF EXISTS '$DBNAME';CREATE DATABASE '$DBNAME';'

# Create SQL table schema and data
python dbimport/metadata.py > $DATADIR/oai-data.sql -i ../data/VG_Variable_tables.bz2
python dbimport/createdb.py -i $DATADIR/OAI/ >> $DATADIR/oai-data.sql

# Load table schema and data
psql -d $DBNAME -f $DATADIR/oai-data.sql
