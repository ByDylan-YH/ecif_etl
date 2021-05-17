#! /bin/bash


etl_date=$1
w_path="bin"

bash ${w_path}/createIndex.sh pty_cert similar_id,group_id,cust_id,cn_name,cert_no,cust_type_cd,src_sys,data_crt_tm,is_merge_flag,cert_type_cd,is_repair_flag,repair_dt,issue_lvl,is_idcard_flag,src_cust_status,unknow_custom_flag,group_id_col,src_sys_col ${etl_date} router.field=solr_cust_id
bash ${w_path}/sync.sh pty_cert ${etl_date}

bash ${w_path}/createIndex.sh pty_cust_flg cust_id,name,cert_type_cd,cert_no ${etl_date} router.field=solr_cust_id
bash ${w_path}/sync.sh pty_cust_flg ${etl_date}

bash ${w_path}/createIndex.sh pty_src_cust_id cust_id,cust_type_cd,src_cust_id,src_sys ${etl_date} router.field=solr_cust_id
bash ${w_path}/sync.sh pty_src_cust_id ${etl_date}