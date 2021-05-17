#! /bin/bash

etl_date=$1
#工作目录
w_path="bin"
#上一天,到时候要复制ecifdb的
last_date=`date -d "${etl_date} -1 day" +%Y%m%d`
#当前namespace
cur_namespace=ecifdb${etl_date}
#前一天namespace
last_namespace=$3
#需要同步的表
arr=$2
OLD_IFS="$IFS"
IFS=","
sync_tables=(${arr})
IFS="$OLD_IFS"
#不需要同步的表
tables=("PTY_AML_RISK" "PTY_STOCK" "PTY_BELONG_INFO" "PTY_CUST_GROUP" "PTY_SIMILAR_CUST" "PTY_INDV" "PTY_INDV_SUB" "PTY_CORP" "PTY_CORP_SUB" "PTY_CERT" "PTY_TEL" "PTY_ADDR" "PTY_NET_ADDR" "PTY_JOB" "PTY_EDU" "PTY_PERFER" "PTY_BANKACCT" "PTY_INCOME_EXPNS" "PTY_CONT" "REL_FAMILY_MEM" "REL_CUST_ORG" "REL_PTY_ACC" "PRJ_BIB_PMPROJ" )
tables_mdt=( "PTY_MAIN_INFO" "PTY_BROAD_INFO" )
#客户识别表的预分区
sync_spilts="'00','05','10','15','20','25','30','35','40','45','50','55','60','65','70','75','80','85','90','95'"
#模型表的预分区
spilts="'0', '1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9' ,'a' ,'b' ,'c' ,'d' ,'e' ,'f'"
#sql语句
sql=""
#临时文件
tmp_file=${w_path}/tmp_hbase.sql

#初始化hbase表
rm -f ${tmp_file}

list_namespace=`hbase shell <<EOF
list_namespace
EOF
`
list_snapshots=`hbase shell <<EOF
list_snapshots
EOF
`

[[ ${list_namespace} =~ ${cur_namespace} ]] || echo "create_namespace '${cur_namespace}'" >> ${tmp_file}


echo "disable_all '${cur_namespace}:.*'" >> ${tmp_file}
echo "y" >> ${tmp_file}
echo "drop_all '${cur_namespace}:.*'" >> ${tmp_file}
echo "y" >> ${tmp_file}
sql=`
    for table in ${tables[@]};do
        echo "create '${cur_namespace}:${table}','f', SPLITS => [${sync_spilts}]" >> ${tmp_file}
    done

    for table in ${tables_mdt[@]};do
        echo "create '${cur_namespace}:${table}','f', SPLITS => [${spilts}]" >> ${tmp_file}
    done

    for table in ${sync_tables[@]};do
        [[ ${list_snapshots} =~ "tableSnapshot_${last_namespace}_${table}" ]] &&
         echo "clone_snapshot 'tableSnapshot_${last_namespace}_${table}', '${cur_namespace}:${table}'" >> ${tmp_file} ||
          echo "create '${cur_namespace}:${table}','f', SPLITS => [${sync_spilts}]" >> ${tmp_file}
    done

    echo "exit" >> ${tmp_file}
`
echo "初始化 hbase表!"
sql=`cat ${tmp_file}`
hbase shell <<EOF
    ${sql}
EOF
#初始化hive外表
echo "初始化hive外表!"
hive -hiveconf namespace=${cur_namespace} -f ${w_path}/init_hive_external_table.sql

# S001, S002, S006, S009产品户清洗
echo "正在执行1269product.sql"
hive -f ${w_path}/1269product.sql

# 证件类型清洗
echo "正在执行id_type_clean.sql"
hive -f ${w_path}/id_type_clean.sql


# 如果active_ns 为null 则用当天的
if [[ ${last_namespace} != "null" ]]; then
    hive -hiveconf namespace=${cur_namespace} -hiveconf active_ns=${last_namespace} -f ${w_path}/pty_cust_info.sql
else
    hive -hiveconf namespace=${cur_namespace} -hiveconf active_ns=${cur_namespace} -f ${w_path}/pty_cust_info.sql
fi

# 查看上面的sql是否都执行成功
if [[ `cat *.log | grep "Status: Failed"` != "" || `cat *.log | grep "FAILED: "` != "" ]]; then
    echo "hive sql 执行出错"
    exit 1
fi

#生成初始化标志
echo "生成初始化标志!"
hdfs dfs -mkdir -p /tmp/ecif
hdfs dfs -test -f /tmp/ecif/${etl_date}.mk
    if [[ ! $? -eq 0 ]]; then
        hdfs dfs -touchz /tmp/ecif/${etl_date}.mk
fi