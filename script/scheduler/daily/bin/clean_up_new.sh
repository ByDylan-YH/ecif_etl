#! /bin/bash


etl_date=$1
last_week=ecifdb`date -d "${etl_date} -6 day" +%Y%m%d`



# 清空hive历史库
hive_databases=`hive -e "show databases"`
for database in ${hive_databases[@]};do
    if [[ ${database} =~ "ecifdb20" && "${database}" < "${last_week}" ]]; then
        hive -e "drop database if exists ${database} cascade"
    fi
done

# 清空hbase历史namespace
hbase_namespace=`hbase shell <<EOF
list_namespace
EOF
`
for namespace in ${hbase_namespace[@]};do
    if [[ ${namespace} =~ "ecifdb20" && "${namespace}" < "${last_week}" ]]; then
        hbase shell <<EOF
        disable_all '${namespace}:.*'
        y
        drop_all '${namespace}:.*'
        y
        drop_namespace '${namespace}'
        delete_all_snapshot 'tableSnapshot_${namespace}_.*'
        y
        exit
EOF
    fi
done

# 清空solr索引
solr_docs=`solrctl collection --list`
for doc in ${solr_docs[@]};do
    if [[ ${doc} =~ "ecifdb20" && ${doc%%_*} < "${last_week}" ]]; then
        echo "sh ./bin/dropIndex.sh ${doc#*_} ${doc:6:8}"
        bash ./bin/dropIndex.sh ${doc#*_} ${doc:6:8}
    fi
done

# 清空标记文件
mark_files=`hdfs dfs -ls /tmp/ecif/ | awk -F'/' '{print $4}'`
for file in ${mark_files[@]};do
    if [[ ${file:0:8} != "19990101" && "ecifdb"${file:0:8} < $last_week ]]; then
         echo "hdfs dfs -rm -f /tmp/ecif/$file"
         hdfs dfs -rm -f /tmp/ecif/$file
    fi
done
#hdfs dfs -rm -f /tmp/ecif/${last_week}.mk