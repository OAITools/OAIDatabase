# convert query to CSV

DBNAME='oai'

psql -d $DBNAME --no-align -F"," -c "$1" > $2
