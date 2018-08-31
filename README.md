# VariousSQLScripts
Bucket for me to toss my various SQL scripts into for later reuse

* Load Control Bucketing for mass parallel movement.sql
** This script requires tables listing all databases and tables that are to be moved.  It estimates the size ( rows * columns ) then trying to split them evenly by size ( and secondarily count ) as evenly as possible in the requested number of buckets.  Prototype for etl process.
