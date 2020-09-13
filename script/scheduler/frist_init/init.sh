#! /bin/bash

etl_date=$1
mysql_user=$2
mysql_pwd=$3
w_path="bin"
if [[ ${etl_date} -eq '' ]]; then
        echo "请输入etl_date!"
        exit
fi
last_date=`date -d "${etl_date} -1 day" +%Y%m%d`

#同步MySQL
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/create_table.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/S001_t_sys_code_sourcerelation.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/S002_t_sys_code_sourcerelation.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/S005_t_sys_code_sourcerelation.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/S006_t_sys_code_sourcerelation.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/S008_t_sys_code_sourcerelation.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/S009_t_sys_code_sourcerelation.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/S010_t_sys_code_sourcerelation.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/t_col_field_priority.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/t_sys_authorityfor_role.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/t_sys_code_detail.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/t_sys_code_type.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/t_sys_page_info.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/t_sys_priority.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/t_sys_priority_status.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/t_sys_role_account.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/t_sys_role_info.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/t_sys_source.sql
mysql -u${mysql_user} -p${mysql_pwd} < ${w_path}/sql/t_sys_subsite_info.sql

#新建hbase前一天的库和表
bash ${w_path}/frist_hbase.sh ${last_date}
