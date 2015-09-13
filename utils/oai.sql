-- *******************************************************************
-- Initialize empty OAI database, 
-- https://oai.epi-ucsf.org/datarelease/
-- @date 2015-28-8
-- @author Jason Alan Fries
-- @email jfries [at] stanford.edu
-- *******************************************************************

--
-- Category Definitions
--
CREATE TABLE categorydefs (
    id SERIAL,
    type INTEGER NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY(id) );
    
--
-- Variable-Category Mappings
--
CREATE TABLE varcategories (
    var_id VARCHAR(20) NOT NULL,
    cat_id INTEGER references categorydefs(id),
    PRIMARY KEY(var_id, cat_id) );
        
CREATE TABLE vardefs (
    var_id VARCHAR(20) NOT NULL,
    type VARCHAR(20) NOT NULL,
    labeln INTEGER,
    labelset TEXT,
    dataset VARCHAR(128),
    collect_form TEXT,
    comment TEXT,
    PRIMARY KEY(var_id) );
		