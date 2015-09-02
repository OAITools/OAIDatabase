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
		var_id VARCHAR(20) NOT NULL,
		type INTEGER NOT NULL,
		name TEXT NOT NULL,
		PRIMARY KEY(var_id, name, type) );
--	
-- DataSet origin and description (Datasets): 
--
CREATE TABLE datasets (
		var_id VARCHAR(20) NOT NULL,
		name VARCHAR(128) NOT NULL,
		collect_form TEXT,
		comment TEXT,
		PRIMARY KEY(var_id) );
		
		