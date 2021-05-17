drop view  if exists ${hiveconf:namespace}.acc_ast_fa;
 create view if not exists ${hiveconf:namespace}.acc_ast_fa(
   group_id comment '群组编号',
   src_cust_id comment '源系统客户号',
   cust_id comment '客户编号',
   acct_id comment '账户编号',
   sys_dt comment '系统日期',
   src_sys comment '来源系统',
   upd_sys comment '更新系统',
   data_crt_tm comment '数据创建时间',
   data_upd_tm comment '数据更新时间',
   etl_crt_tm comment 'ETL创建时间',
   etl_upd_tm comment 'ETL更新时间'
)
 as 
  select
   `group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1604$_greisen_sys_date` as sys_dt,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from 
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1604';

 drop view  if exists ${hiveconf:namespace}.acc_ast_td;
 create view if not exists ${hiveconf:namespace}.acc_ast_td(
   group_id comment '群组编号',
   src_cust_id comment '源系统客户号',
   cust_id comment '客户编号',
   acct_id comment '账户编号',
   sys_dt comment '系统日期',
   acct_name comment '账户名称',
   trad_acct_cd comment '交易帐号卡号',
   main_acct_flag comment '主账号标识',
   src_sys comment '来源系统',
   upd_sys comment '更新系统',
   data_crt_tm comment '数据创建时间',
   data_upd_tm comment '数据更新时间',
   etl_crt_tm comment 'ETL创建时间',
   etl_upd_tm comment 'ETL更新时间'
)
 as 
  select
   `group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1605$_greisen_sys_date` as sys_dt,
   `B$1605$_greisen_trad_acct_name` as acct_name,
   `B$1605$_greisen_trad_acct_card` as trad_acct_cd,
   `B$1605$_greisen_main_acct_flag` as main_acct_flag,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from 
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1605';

 drop view  if exists ${hiveconf:namespace}.acc_ast_bk;
 create view if not exists ${hiveconf:namespace}.acc_ast_bk(
   group_id comment '群组编号',
   src_cust_id comment '源系统客户号',
   cust_id comment '客户编号',
   acct_id comment '账户编号',
   acct_stat comment '帐号状态',
   acct_owner_name comment '银行户名',
   fund_mode comment '资金方式',
   bank_open_name comment '开户银行名称',
   src_sys comment '来源系统',
   upd_sys comment '更新系统',
   data_crt_tm comment '数据创建时间',
   data_upd_tm comment '数据更新时间',
   etl_crt_tm comment 'ETL创建时间',
   etl_upd_tm comment 'ETL更新时间'
)
 as 
  select
   `group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `C$1606$_greisen_acct_stat` as acct_stat,
   `C$1606$_greisen_bank_acct_owner` as acct_owner_name,
   `C$1606$_greisen_fund_mode` as fund_mode,
   `C$1606$_greisen_open_bank` as bank_open_name,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from 
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1606';
  
  drop view  if exists ${hiveconf:namespace}.acc_ast_bd;
 create view if not exists ${hiveconf:namespace}.acc_ast_bd(
   group_id comment '群组编号',
   src_cust_id comment '源系统客户号',
   cust_id comment '客户编号',
   acct_id comment '账户编号',
   acct_stat comment '帐号状态',
   acct_name comment '证券账号名称',
   src_sys comment '来源系统',
   upd_sys comment '更新系统',
   data_crt_tm comment '数据创建时间',
   data_upd_tm comment '数据更新时间',
   etl_crt_tm comment 'ETL创建时间',
   etl_upd_tm comment 'ETL更新时间'
)
 as 
  select
   `group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `acct_stat_cd` as acct_stat,
   `B$2301$_bond_acct_name` as acct_name,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from 
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='2301';