#! /bin/bash 


table=$1
etl_date=ecifdb$2
w_path="bin"
conf_path=${w_path}/../conf/${table}_${etl_date}/
mor_path="${conf_path}/morphlines.conf"

jar_path=`sh ${w_path}/chooseEnv.sh \
/opt/cloudera/parcels/CDH-5.14.2-1.cdh5.14.2.p0.3/jars/hbase-indexer-mr-1.5-cdh5.14.2-job.jar \
/home/cdh/cloudera/parcels/CDH-5.14.2-1.cdh5.14.2.p0.3/jars/hbase-indexer-mr-1.5-cdh5.14.2-job.jar`

zk_host=`sh ${w_path}/chooseEnv.sh hadoop3:2181/solr ecif-cdh-prod-node3:2181/solr`

hadoop --config /etc/hadoop/conf \
    jar ${jar_path} \
    --conf /etc/hbase/conf.cloudera.hbase/hbase-site.xml \
    --hbase-indexer-file ${conf_path}/morphline-hbase-mapper.xml \
    --zk-host ${zk_host} \
    --collection ${etl_date}_${table} \
    --morphline-file ${mor_path} \
    --reducers 0 \
    --go-live

