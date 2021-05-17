#! /bin/bash


etl_date=$1
if [[ ${etl_date} -eq '' ]]; then
        echo "请输入etl_date!"
        exit
fi
#工作目录
w_path="bin"
# MySQL 主机
#MHOST=$2
# 端口
#MPOST=$3
# MySQL 用户名
#MUSER=$4
# MySQL 密码
#MPSWD=$5
# yyyy-MM-dd格式的etl_date
date=`date -d ${etl_date}  "+%Y-%m-%d"`
last_date=`date -d "${etl_date} -1 day" +%Y%m%d`
active_namespace=$3

#查询mysql 看今天是否已经初始化
#res=$(mysql -h${MHOST} -P${MPOST} -u${MUSER} -p${MPSWD} -s -e "select count(1) from ecifdb.a_etl_jobinfo where rqbdate='${date}'")


sync_table="PTY_CUST_FLG,FR_SEQ,GR_SEQ,LS_SEQ,SIMILAR_SEQ,PTY_MERGE_STATUS,PTY_SRC_CUST_ID"

{
    #初始化
    if [[ $2 == "delAll" ]]; then
        echo "正在进行强制重跑, 清空当天数据重跑! 重跑日期: ${etl_date}"
        bash ${w_path}/run.sh 0 -1 ${etl_date}  &&
        bash ${w_path}/daily_init.sh ${etl_date} ${sync_table} ${active_namespace}
    fi

    if [[ ! $? -eq 0 ]]; then
        echo "初始化失败!"
        echo "MISSION FAILED!"
        exit 1
    fi

    hdfs dfs -test -f /tmp/ecif/${etl_date}.mk
    if [[ ! $? -eq 0 ]]; then
        echo "第一次执行 ${etl_date} 任务..."
        bash ${w_path}/daily_init.sh ${etl_date} ${sync_table} ${active_namespace}
    else
        echo "自动重跑, 清空索引!"
        bash ${w_path}/dropAll.sh ${etl_date}
    fi &&
    #spark
    echo "执行 spark 任务! " &&
    echo "${w_path}/run.sh 0 0 ${etl_date} $2" &&
    bash ${w_path}/run.sh 0 0 ${etl_date} $2 &&
    #执行hive sql
    echo "执行hive sql" &&
    bash ${w_path}/exec_hive_sql.sh ${etl_date} &&
    #backup
    echo "备份快照!" &&
    bash ${w_path}/back_up.sh ${etl_date} ${sync_table} &&
    #同步solr索引
    echo "同步索引!" &&
    bash ${w_path}/all_sync.sh ${etl_date} &&
    #清理历史数据
    echo "清理历史数据!" &&
    bash ${w_path}/clean_up_new.sh  ${etl_date}
}&&{
    echo "MISSION SUCCESS!"
}||{
    echo "MISSION FAILED!"
    exit 1
}
