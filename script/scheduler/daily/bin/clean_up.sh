#! /bin/bash


etl_date=$1
last_week=`date -d "${etl_date} -7 day" +%Y%m%d`
w_path="bin"
tmp_file=${w_path}/tmp_clean_hbase.sql
del_namespace=ecifdb${last_week}



hbase shell <<EOF

disable_all '${del_namespace}:.*'
y
drop_all '${del_namespace}:.*'
y
drop_namespace '${del_namespace}'
delete_all_snapshot 'tableSnapshot_${del_namespace}_.*'
exit
EOF

hive <<EOF
drop database if exists ${del_namespace} cascade;
exit;
EOF

hdfs dfs -rm -f /tmp/ecif/${last_week}.mk