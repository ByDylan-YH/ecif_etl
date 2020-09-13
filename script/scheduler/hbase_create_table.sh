#! /bin/bash
tables=("PTY_SIMILAR_CUST" "PTY_CUST_GROUP" "PTY_CUST_FLG" "PTY_INDV" "PTY_INDV_SUB" "PTY_CORP" "PTY_CORP_SUB" "PTY_CERT" "PTY_TEL" "PTY_ADDR" "PTY_NET_ADDR" "PTY_JOB" "PTY_EDU" "PTY_PERFER" "PTY_BANKACCT" "PTY_INCOME_EXPNS" "PTY_CONT" "REL_FAMILY_MEM" "REL_CUST_ORG" "REL_PTY_ACC" "PRJ_BIB_PMPROJ" "PTY_MAIN_INFO" "PTY_BROAD_INFO" "FR_SEQ" "GR_SEQ" "LS_SEQ" "SIMILAR_SEQ" "PTY_MERGE_STATUS" "PTY_SRC_CUST_ID")
sql=""
tmp_name_space=ecifdb20181114
prod_name_space=ecifdb
is_first=true
spilts="'0', '1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9' ,'a' ,'b' ,'c' ,'d' ,'e' ,'f'"

if [[ ${is_first} = false ]];then
  t1="PTY_CUST_FLG"
  t2="PTY_SRC_CUST_ID"
fi

# 建表
rm -f hbase_create.sql
if [[ "$1" -eq "1" ]]; then
    echo "create_namespace '${tmp_name_space}'" >> hbase_create.sql
 sql=`
    for table in ${tables[@]};do
        echo "create '${tmp_name_space}:${table}',{NAME => 'f', REPLICATION_SCOPE => 1}, SPLITS => [${spilts}]" >> hbase_create.sql
    done
`
elif [[ "$1" -eq "2" ]]; then
     sql=`
    for table in ${tables[@]};do
        echo "disable '${tmp_name_space}:${table}'" >> hbase_create.sql
        echo "drop '${tmp_name_space}:${table}'" >> hbase_create.sql
    done
`
elif [[ "$1" -eq "3" ]]; then
    for table in ${tables[@]};do
            if [[ ${table} = "PTY_MERGE_STATUS" || ${table} = $t1 || ${table} = $t2 ]]; then
               echo ""
            else
               echo "truncate '${tmp_name_space}:${table}'" >> hbase_create.sql
            fi
    done

else
     echo "请输入参数,1建表,2删表,3清空表"
fi

echo "exit" >> hbase_create.sql

cat hbase_create.sql
hbase shell hbase_create.sql
