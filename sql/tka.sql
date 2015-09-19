-- Right or Left TKA subjects and their corresponding pre/post visit dates.
-- Return subjects must have both a pre and post visit. 
-- QUERY RESULTS:
-- id, left-pre, left-post, right-pre, right-post
SELECT A.id,velkvspr,velkvsaf,verkvspr,verkvsaf 
FROM (SELECT id,velkvspr,velkvsaf,verkvspr,verkvsaf
	  FROM outcomes) AS A, 
	 (SELECT id 
	  FROM outcomes 
	  WHERE verkfldt IS NOT NULL OR velkfldt IS NOT NULL) AS B 
WHERE A.id=B.id AND 
	(velkvspr IS NOT NULL AND velkvsaf IS NOT NULL) OR
	(verkvspr IS NOT NULL AND verkvsaf IS NOT NULL);