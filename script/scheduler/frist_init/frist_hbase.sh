#! /bin/bash
sync_tables=( "PTY_CUST_GROUP" "PTY_CUST_FLG" "FR_SEQ" "GR_SEQ" "LS_SEQ" "SIMILAR_SEQ" "PTY_MERGE_STATUS" "PTY_SRC_CUST_ID" )
sql=""
tmp_name_space=ecifdb$1
sync_spilts="'0', '1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9'"
w_path="bin"
tmp_file=${w_path}/hbase_create.sql


# 建表
rm -f ${tmp_file}

echo "create_namespace '${tmp_name_space}'" >> ${tmp_file}
sql=`
    for table in ${sync_tables[@]};do
        echo "create '${tmp_name_space}:${table}',{NAME => 'f', REPLICATION_SCOPE => 1}, SPLITS => [${sync_spilts}]" >> ${tmp_file}
    done
`


echo "exit" >> ${tmp_file}

cat ${tmp_file}
hbase shell ${tmp_file}
