-- *******************************************************************
-- Initialize empty OAI database, 
-- http://www.fda.gov/Drugs/InformationOnDrugs/ucm079750.htm
-- @retrieved 2015-28-8
-- @author Jason Alan Fries
-- @email jfries [at] stanford.edu
-- *******************************************************************

--
-- Variable Categories and Subcategories (Categories): 
--
CREATE TABLE categories (
		var_name VARCHAR(20) NOT NULL,
		cat_type INTEGER NOT NULL,
		cat_name TEXT NOT NULL,
		PRIMARY KEY(var_name, cat_name, cat_type) );
--	
-- DataSet origin and description (Datasets): 
--
CREATE TABLE datasets (
		var_name VARCHAR(20) NOT NULL,
		dataset VARCHAR(128) NOT NULL,
		collect_form TEXT,
		comment TEXT,
		PRIMARY KEY(var_name) );
		
		