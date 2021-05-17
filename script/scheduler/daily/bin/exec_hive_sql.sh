#! /bin/bash

etl_date=$1
#工作目录
w_path="bin"
#当前namespace
cur_namespace=ecifdb${etl_date}

hive -hiveconf namespace=${cur_namespace} -f ${w_path}/ecif_acc_view.sql
hive -hiveconf namespace=${cur_namespace} -f ${w_path}/ecif_prj_view.sql
hive -hiveconf namespace=${cur_namespace} -f ${w_path}/ecif_acc_s007_view.sql
hive -hiveconf namespace=${cur_namespace} -f ${w_path}/init_hive_modle_view.sql
