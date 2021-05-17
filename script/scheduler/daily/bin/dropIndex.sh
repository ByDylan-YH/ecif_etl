#! /bin/bash 


name=$1
etl_date=ecifdb$2


#hbase-indexer delete-indexer -n ${etl_date}_${name}_indexer
solrctl collection --delete ${etl_date}_$name
solrctl instancedir --delete dir_${name}_${etl_date}
