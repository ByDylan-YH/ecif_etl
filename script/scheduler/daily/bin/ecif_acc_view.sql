 drop view if exists ${hiveconf:namespace}.acc_info;
 create view if not exists ${hiveconf:namespace}.acc_info (
 group_id comment '群组编号',
 src_cust_id comment '源系统客户号',
 cust_id comment '客户编号',
 acct_id comment '账户编号',
 acct_stat_cd comment '账户状态代码',
 open_dt comment '开户日期',
 cancel_dt comment '销户日期',
 bk_type_cd comment '业务类别代码',
 acct_type_cd comment '账户类型',
 acct_flag_cd comment '账户标志',
 cust_type_cd comment '客户类型代码',
 src_sys comment '来源系统',
 upd_sys comment '更新系统',
 data_crt_tm comment '数据创建时间',
 data_upd_tm comment '数据更新时间',
 etl_crt_tm comment 'ETL创建时间',
 etl_upd_tm comment 'ETL更新时间') COMMENT '账户总表'
 as
  select distinct
   `group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `acct_stat_cd` as acct_stat_cd,
   `open_dt` as open_dt,
   `cancel_dt` as cancel_dt,
   `bk_type_cd` as bk_type_cd,
   `acct_type_cd` as acct_type_cd,
   `acct_flag_cd` as acct_flag_cd,
   `cust_type_cd` as cust_type_cd,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full;



 drop view if exists ${hiveconf:namespace}.acc_bank_futs ;
 create view if not exists ${hiveconf:namespace}.acc_bank_futs (
group_id comment '群组编号',
 src_cust_id comment '源系统客户号',
 cust_id comment '客户编号',
 acct_id comment '账户编号',
 bank_no comment '银行代码',
 currency_cd comment '币种代码',
 asset_acct comment '资产账户',
 bank_acct_flag comment '银行账户标志',
 bank_open_name comment '银行开户名称',
 bank_open_bank comment '银行开户银行',
 futs_settle_acct_type_cd comment '结算账户类型代码',
 src_sys comment '来源系统',
 upd_sys comment '更新系统',
 data_crt_tm comment '数据创建时间',
 data_upd_tm comment '数据更新时间',
 etl_crt_tm comment 'ETL创建时间',
 etl_upd_tm comment 'ETL更新时间') COMMENT '兴证期货结算账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `bank_no` as bank_no,
   `currency_cd` as currency_cd,
   `C$1601$_futs_asset_acct` as asset_acct,
   `C$1601$_futs_bank_acct_flag` as bank_acct_flag,
   `C$1601$_futs_bank_open_name` as bank_open_name,
   `C$1601$_futs_bank_open_bank` as bank_open_bank,
   `C$1601$_futs_settle_acct_type_cd` as futs_settle_acct_type_cd,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1601';



 drop view if exists ${hiveconf:namespace}.acc_bank_tpd;
 create view if not exists ${hiveconf:namespace}.acc_bank_tpd (
 group_id comment '群组编号',
  src_cust_id comment '源系统客户号',
  cust_id comment '客户编号',
  acct_id comment '账户编号',
  bank_no comment '银行代码',
  currency_cd comment '币种代码',
  asset_acct comment '资产账户',
  bank_acct_type_cd comment '银行帐号类别代码',
  main_acct_flag comment '主账户标志',
  branch_no comment '营业部号',
  cust_acct comment '客户账户',
  holder_name comment '股东名称',
  foreign_flag comment '境外标志',
  cert_type_cd comment '证件类别代码',
  cert_no comment '证件号码',
  auth_no comment '自有号',
  out_amt_upper_limit comment '转出金额上限',
  out_amt_lower_limit comment '转出金额下限',
  in_amt_upper_limit comment '转进金额上限',
  in_amt_lower_limit comment '转进金额下限',
  con_tpl comment '控制模板',
  bank_operator comment '银行操作员',
  bank_appt_flag comment '存管指定标志',
  perm_entrust_mode comment '允许委托方式',
  bank_acct_right comment '银行账户权限',
  bank_acct_lmt comment '银行账户限制',
  bank_book_acct comment '银行簿记账户',
  prov_branch_cd comment '省分行代码',
  city_branch_cd comment '市分行代码',
  county_branch_cd comment '县分行代码',
  sub_branch_cd comment '子代码',
  posit_str comment '定位串',
  remark comment '备注',
  src_sys comment '来源系统',
  upd_sys comment '更新系统',
  data_crt_tm comment '数据创建时间',
  data_upd_tm comment '数据更新时间',
  etl_crt_tm comment 'ETL创建时间',
  etl_upd_tm comment 'ETL更新时间') COMMENT '经纪业务三方存管账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `bank_no` as bank_no,
   `currency_cd` as currency_cd,
   `C$1001$_tpd_asset_acct` as asset_acct,
   `C$1001$_tpd_bank_acct_type_cd` as bank_acct_type_cd,
   `C$1001$_tpd_main_acct_flag` as main_acct_flag,
   `C$1001$_tpd_branch_no` as branch_no,
   `C$1001$_tpd_cust_acct` as cust_acct,
   `C$1001$_tpd_holder_name` as holder_name,
   `C$1001$_tpd_foreign_flag` as foreign_flag,
   `C$1001$_tpd_cert_type_cd` as cert_type_cd,
   `C$1001$_tpd_cert_no` as cert_no,
   `C$1001$_tpd_auth_no` as auth_no,
   `C$1001$_tpd_out_amt_upper_limit` as out_amt_upper_limit,
   `C$1001$_tpd_out_amt_lower_limit` as out_amt_lower_limit,
   `C$1001$_tpd_in_amt_upper_limit` as in_amt_upper_limit,
   `C$1001$_tpd_in_amt_lower_limit` as in_amt_lower_limit,
   `C$1001$_tpd_con_tpl` as con_tpl,
   `C$1001$_tpd_bank_operator` as bank_operator,
   `C$1001$_tpd_bank_appt_flag` as bank_appt_flag,
   `C$1001$_tpd_perm_entrust_mode` as perm_entrust_mode,
   `C$1001$_tpd_bank_acct_juri` as bank_acct_right,
   `C$1001$_tpd_bank_acct_lmt` as bank_acct_lmt,
   `C$1001$_tpd_bank_book_account` as bank_book_acct,
   `C$1001$_tpd_prov_branch_cd` as prov_branch_cd,
   `C$1001$_tpd_city_branch_cd` as city_branch_cd,
   `C$1001$_tpd_county_branch_cd` as county_branch_cd,
   `C$1001$_tpd_sub_branch_cd` as sub_branch_cd,
   `C$1001$_tpd_posit_str` as posit_str,
   `C$1001$_tpd_remark` as remark,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1001';



 drop view if exists ${hiveconf:namespace}.acc_ast_futso ;
 create view if not exists ${hiveconf:namespace}.acc_ast_futso (
group_id comment '群组编号',
 src_cust_id comment '源系统客户号',
 cust_id comment '客户编号',
 acct_id comment '账户编号',
 exchange_flag comment '交易所标志',
 freeze_stat comment '休眠状态',
 src_sys comment '来源系统',
 upd_sys comment '更新系统',
 data_crt_tm comment '数据创建时间',
 data_upd_tm comment '数据更新时间',
 etl_crt_tm comment 'ETL创建时间',
 etl_upd_tm comment 'ETL更新时间') COMMENT '兴证期货期权期货账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1602$_futso_exchange_flag` as exchange_flag,
   `B$1602$_futso_freezestatus` as freeze_stat,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1602';



 drop view if exists ${hiveconf:namespace}.acc_ast_futsx ;
 create view if not exists ${hiveconf:namespace}.acc_ast_futsx (
group_id comment '群组编号',
 src_cust_id comment '源系统客户号',
 cust_id comment '客户编号',
 acct_id comment '账户编号',
 exchange_flag comment '交易所标志',
 src_sys comment '来源系统',
 upd_sys comment '更新系统',
 data_crt_tm comment '数据创建时间',
 data_upd_tm comment '数据更新时间',
 etl_crt_tm comment 'ETL创建时间',
 etl_upd_tm comment 'ETL更新时间') COMMENT '兴证期货现货账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1603$_futsx_exchange_flag` as exchange_flag,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1603';



 drop view if exists ${hiveconf:namespace}.acc_ast_ord ;
 create view if not exists ${hiveconf:namespace}.acc_ast_ord (
group_id comment '群组编号',
 src_cust_id comment '源系统客户号',
 cust_id comment '客户编号',
 acct_id comment '账户编号',
 branch_no comment '营业部号',
 fund_card comment '资金卡',
 main_acct_flag comment '主账户标志',
--  risk_level comment '风险级别',
--  risk_end_dt comment '风险截止日期',
 inter_asset_acct comment '内部资产账户',
 cust_grp comment '客户群组',
 room_divide comment '客户分组',
 asset_prop comment '资产属性',
 fare_type_str comment '费用类型串',
 dist_tpl comment '折扣模板',
 perm_entrust_mode comment '允许委托模式',
 cust_rights comment '客户权限',
 cust_restriction comment '客户限制',
 profit_cal_mode comment '盈亏计算模式',
 product_flag comment '产品标志',
 limit_flag comment '限制标志',
 src_sys comment '来源系统',
 upd_sys comment '更新系统',
 data_crt_tm comment '数据创建时间',
 data_upd_tm comment '数据更新时间',
 etl_crt_tm comment 'ETL创建时间',
 etl_upd_tm comment 'ETL更新时间') COMMENT '经纪业务普通账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1002$_ord_acct_branch_no` as branch_no,
   `B$1002$_ord_acct_fund_card` as fund_card,
   `B$1002$_ord_acct_main_acct_flag` as main_acct_flag,
--    `B$1002$_ord_acct_risk_level` as risk_level,
--    `B$1002$_ord_acct_risk_end_dt` as risk_end_dt,
   `B$1002$_ord_acct_inter_asset_acct` as inter_asset_acct,
   `B$1002$_ord_acct_ust_grp` as cust_grp,
   `B$1002$_ord_acct_room_code` as room_divide,
   `B$1002$_ord_acct_asset_prop` as asset_prop,
   `B$1002$_ord_acct_fare_type_str` as fare_type_str,
   `B$1002$_ord_acct_dist_model` as dist_tpl,
   `B$1002$_ord_acct_perm_entrust_mode` as perm_entrust_mode,
   `B$1002$_ord_acct_cust_rights` as cust_rights,
   `B$1002$_ord_acct_cust_restriction` as cust_restriction,
   `B$1002$_ord_acct_profit_cal_mode` as profit_cal_mode,
   `B$1002$_ord_acct_product_flag` as product_flag,
   `B$1002$_ord_acct_limit_flag` as limit_flag,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1002';



 drop view if exists ${hiveconf:namespace}.acc_ast_mt ;
 create view if not exists ${hiveconf:namespace}.acc_ast_mt (
 group_id comment '群组编号',
  src_cust_id comment '源系统客户号',
  cust_id comment '客户编号',
  acct_id comment '账户编号',
  asset_acct comment '资产账户',
  branch_no comment '营业部号',
  fi_quota comment '融资额度（元）',
  fs_quota comment '融券额度（元）',
  max_quota comment '最大额度（元）',
  fi_rate comment '融资利率（%）',
  fs_rate comment '融券利率（%）',
  ctr_begin_dt comment '合同开始日期',
  ctr_end_dt comment '合同截止日期',
  src_sys comment '来源系统',
  upd_sys comment '更新系统',
  data_crt_tm comment '数据创建时间',
  data_upd_tm comment '数据更新时间',
  etl_crt_tm comment 'ETL创建时间',
  etl_upd_tm comment 'ETL更新时间') COMMENT '经纪业务融资融券账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1003$_mt_mt_asset_acct` as asset_acct,
   `B$1003$_mt_branch_no` as branch_no,
   `B$1003$_mt_fi__quota` as fi_quota,
   `B$1003$_mt_ma_quota` as fs_quota,
   `B$1003$_mt_max_quota` as max_quota,
   `B$1003$_mt_fi_rate` as fi_rate,
   `B$1003$_mt_fs_rate` as fs_rate,
   `B$1003$_mt_ctr_begin_dy` as ctr_begin_dt,
   `B$1003$_mt_ctr_end_date` as ctr_end_dt,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1003';



 drop view if  exists ${hiveconf:namespace}.acc_ast_iso ;
 create view if not exists ${hiveconf:namespace}.acc_ast_iso (
 group_id comment '群组编号',
  src_cust_id comment '源系统客户号',
  cust_id comment '客户编号',
  acct_id comment '账户编号',
  asset_acct comment '资产账户',
  branch_no comment '营业部号',
  cust_opt_level comment '客户期权等级',
  power_hold_quota comment '权力持仓额度',
  sh_pur_quota comment '上海限购额度（元）',
  total_hold_quota comment '总持仓限额（元）',
  sh_pur_quota_expire_dt comment '上海限购额度到期日期',
  td_sum_buy_quota comment '当日累计买入额度',
  src_sys comment '来源系统',
  upd_sys comment '更新系统',
  data_crt_tm comment '数据创建时间',
  data_upd_tm comment '数据更新时间',
  etl_crt_tm comment 'ETL创建时间',
  etl_upd_tm comment 'ETL更新时间') COMMENT '经纪业务个股期权账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1004$_iso_iso_asset_acct` as asset_acct,
   `B$1004$_iso_branch_no` as branch_no,
   `B$1004$_iso_cust_opt_level` as cust_opt_level,
   `B$1004$_iso_right_hold_quota` as power_hold_quota,
   `B$1004$_iso_sh_pur_quota` as sh_pur_quota,
   `B$1004$_iso_total_hold_quota` as total_hold_quota,
   `B$1004$_iso_lop_expire_date` as sh_pur_quota_expire_dt,
   `B$1004$_iso_td_sum_buy_quota` as td_sum_buy_quota,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1004';





 drop view if exists ${hiveconf:namespace}.acc_ast_bf ;
 create view if not exists ${hiveconf:namespace}.acc_ast_bf (
group_id comment '群组编号',
 src_cust_id comment '源系统客户号',
 cust_id comment '客户编号',
 acct_id comment '账户编号',
 asset_acct comment '资产账户',
 prod_ta_no comment '产品TA号',
 bank_fina_acct comment '银行理财账户',
 branch_no comment '营业部号',
 cert_type_cd comment '证件类别代码',
 cert_no comment '证件号码',
 prod_acct_stat comment '产品账户状态',
 dividend_mode comment '分红方式',
 seat_no comment '席位号',
 posi_str comment '定位串',
 ta_risk_level comment 'TA风险级别',
 risk_end_dt comment '风险截止日期',
 trans_acct comment '交易帐号',
 operator_name comment '操作人名称',
 agent_cert_type_cd comment '经办人证件类型代码',
 agent_cert_no comment '经办人证件号码',
 src_sys comment '来源系统',
 upd_sys comment '更新系统',
 data_crt_tm comment '数据创建时间',
 data_upd_tm comment '数据更新时间',
 etl_crt_tm comment 'ETL创建时间',
 etl_upd_tm comment 'ETL更新时间') COMMENT '经纪业务银行理财账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1005$_bf_cft_acct` as asset_acct,
   `B$1005$_bf_prod_ta_no` as prod_ta_no,
   `B$1005$_bf_bf_acct` as bank_fina_acct,
   `B$1005$_bf_branch_no` as branch_no,
   `B$1005$_bf_cert_type_cd` as cert_type_cd,
   `B$1005$_bf_cert_no` as cert_no,
   `B$1005$_bf_prod_acct_stat` as prod_acct_stat,
   `B$1005$_bf_dividend_mode` as dividend_mode,
   `B$1005$_bf_seat_no` as seat_no,
   `B$1005$_bf_posi_str` as posi_str,
   `B$1005$_bf_ta_risk_level` as ta_risk_level,
   `B$1005$_bf_risk_end_dt` as risk_end_dt,
   `B$1005$_bf_trans_acct` as trans_acct,
   `B$1005$_bf_operator_name` as operator_name,
   `B$1005$_bf_agent_cert_type_cd` as agent_cert_type_cd,
   `B$1005$_bf_agent_cert_no` as agent_cert_no,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1005';



 drop view if exists ${hiveconf:namespace}.acc_ast_sf ;
 create view if not exists ${hiveconf:namespace}.acc_ast_sf (
group_id comment '群组编号',
 src_cust_id comment '源系统客户号',
 cust_id comment '客户编号',
 acct_id comment '账户编号',
 asset_acct comment '资产账户',
 prod_ta_no comment '产品TA编号',
 secu_fina_acct comment '证券理财账户',
 branch_no comment '营业部号',
 cert_type_cd comment '证件类别代码',
 cert_no comment '证件号码',
 prod_holder_stat comment '产品账户状态',
 dividend_mode comment '分红方式',
 remark comment '备注',
 seat_no comment '席位号',
 export_dt comment '导出日期',
 posi_str comment '定位串',
 prod_holder_type_cd comment '产品持有类型代码',
 trans_acct comment '交易帐号',
 register_flag comment '登记标志',
 secu_fina_acct_right comment '证券理财账户权限',
 pay_acct comment '支付账户',
 pay_type_cd comment '支付方式代码',
 bank_cd comment '银行代码',
 src_sys comment '来源系统',
 upd_sys comment '更新系统',
 data_crt_tm comment '数据创建时间',
 data_upd_tm comment '数据更新时间',
 etl_crt_tm comment 'ETL创建时间',
 etl_upd_tm comment 'ETL更新时间') COMMENT '经纪业务证券理财账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1006$_sf_sf_asset_acct` as asset_acct,
   `B$1006$_sf_prod_ta_no` as prod_ta_no,
   `B$1006$_sf_sf_acct` as shares_fina_acct,
   `B$1006$_sf_branch_no` as branch_no,
   `B$1006$_sf_cert_type_cd` as cert_type_cd,
   `B$1006$_sf_cert_no` as cert_no,
   `B$1006$_sf_prod_holder_stat` as prod_holder_stat,
   `B$1006$_sf_dividend_mode` as dividend_mode,
   `B$1006$_sf_remark` as remark,
   `B$1006$_sf_seat_no` as seat_no,
   `B$1006$_sf_export_dt` as export_dt,
   `B$1006$_sf_posi_str` as posi_str,
   `B$1006$_sf_prod_holder_type_cd` as prod_holder_type_cd,
   `B$1006$_sf_trans_acct` as trans_acct,
   `B$1006$_sf_register_flag` as register_flag,
   `B$1006$_sf_acct_right` as shares_fina_acct_right,
   `B$1006$_sf_pay_acct` as pay_acct,
   `B$1006$_sf_pay_type_cd` as pay_type_cd,
   `B$1006$_sf_bank_cd` as bank_cd,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1006';



 drop view if exists ${hiveconf:namespace}.acc_ast_scc ;
 create view if not exists ${hiveconf:namespace}.acc_ast_scc (
group_id comment '群组编号',
 src_cust_id comment '源系统客户号',
 cust_id comment '客户编号',
 acct_id comment '账户编号',
 cust_group comment '客户群组',
 com_clf comment '公司分类',
 bk_clf comment '业务分类',
 acct_type_cd comment '帐户类别代码',
 main_acct comment '主账号',
 src_sys comment '来源系统',
 upd_sys comment '更新系统',
 data_crt_tm comment '数据创建时间',
 data_upd_tm comment '数据更新时间',
 etl_crt_tm comment 'ETL创建时间',
 etl_upd_tm comment 'ETL更新时间') COMMENT '海交中心资产账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1801$_scc_cust_group` as cust_group,
   `B$1801$_scc_com_clf` as com_clf,
   `B$1801$_scc_bk_clf` as bk_clf,
   `B$1801$_scc_acct_type_cd` as acct_type_cd,
   `B$1801$_scc_main_acct` as main_acct,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1801';




 drop view if  exists ${hiveconf:namespace}.acc_ast_tg ;
 create view if not exists ${hiveconf:namespace}.acc_ast_tg (
group_id comment '群组编号',
 src_cust_id comment '源系统客户号',
 cust_id comment '客户编号',
 acct_id comment '账户编号',
 fund_acct_no comment '资金账户号',
 prod_id comment '产品编号',
 prod_cd comment '产品代码',
 prod_abbr comment '产品简称',
 fund_acct_type_cd comment '资金账户类型代码',
 prod_stat comment '产品状态',
 prod_est_dt comment '产品成立日期',
 src_sys comment '来源系统',
 upd_sys comment '更新系统',
 data_crt_tm comment '数据创建时间',
 data_upd_tm comment '数据更新时间',
 etl_crt_tm comment 'ETL创建时间',
 etl_upd_tm comment 'ETL更新时间') COMMENT '托管业务资产账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1401$_tg_fund_acct_no` as fund_acct_no,
   `B$1401$_tg_prod_id` as prod_id,
   `B$1401$_tg_prod_cd` as prod_cd,
   `B$1401$_tg_prod_sname` as prod_abbr,
   `B$1401$_tg_fund_acct_type_cd` as fund_acct_type_cd,
   `B$1401$_tg_prot_stat` as prod_stat,
   `B$1401$_tg_prot_est_dt` as prod_est_dt,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1401';




 drop view if exists ${hiveconf:namespace}.acc_ats_tas ;
 create view if not exists ${hiveconf:namespace}.acc_ats_tas (
group_id comment '群组编号',
 src_cust_id comment '源系统客户号',
 cust_id comment '客户编号',
 acct_id comment '账户编号',
 seller_no comment '销售商号',
 dot_no comment '网点号',
 trans_acct comment '交易账户',
 open_dot_flag comment '开户网点标志',
 acct_default_dividend_mode comment '账户默认分红方式',
 bank_name comment '银行名称',
 bank_acct comment '银行账户',
 bank_acct_name comment '银行账户名称',
 operation_dot_no comment '操作网点号',
 trans_acct_bak comment '交易账户备份',
 special_acct_flag comment '特殊账户标志',
 special_acct_id comment '特殊账户编号',
 his_flag comment '历史标志',
 src_sys comment '来源系统',
 upd_sys comment '更新系统',
 data_crt_tm comment '数据创建时间',
 data_upd_tm comment '数据更新时间',
 etl_crt_tm comment 'ETL创建时间',
 etl_upd_tm comment 'ETL更新时间') COMMENT '兴证资管分TA资产账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1701$_tas_seller_no` as seller_no,
   `B$1701$_tas_dot_no` as dot_no,
   `B$1701$_tas_trans_acct` as trans_acct,
   `B$1701$_tas_open_dot_flag` as open_dot_flag,
   `B$1701$_tas_acct_default_dividend_mode` as acct_default_dividend_mode,
   `B$1701$_tas_c_bank_name` as bank_name,
   `B$1701$_tas_bank_acct` as bank_acct,
   `B$1701$_tas_bank_acct_name` as bank_acct_name,
   `B$1701$_tas_operation_dot_no` as operation_dot_no,
   `B$1701$_tas_trans_acct_bak` as trans_acct_bak,
   `B$1701$_tas_special_acct_flag` as special_acct_flag,
   `B$1701$_tas_special_acct_id` as special_acct_id,
   `B$1701$_tas_his_flag` as his_flag,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1701';




 drop view if exists ${hiveconf:namespace}.acc_ast_stas ;
 create view if not exists ${hiveconf:namespace}.acc_ast_stas (
group_id comment '群组编号',
 src_cust_id comment '源系统客户号',
 cust_id comment '客户编号',
 acct_id comment '账户编号',
 seller_no comment '销售商编号',
 dot_no comment '网点编号',
 trans_acct comment '交易账户',
 trans_acct_stat comment '交易账户状态',
 acct_default_dividend_mode comment '账户默认分红方式',
 bank_name comment '银行名称',
 bank_acct comment '银行账户',
 bank_acct_name comment '银行账户名称',
 operation_dot_no comment '操作网点编号',
 trans_acct_bak comment '交易账户备份',
 src_sys comment '来源系统',
 upd_sys comment '更新系统',
 data_crt_tm comment '数据创建时间',
 data_upd_tm comment '数据更新时间',
 etl_crt_tm comment 'ETL创建时间',
 etl_upd_tm comment 'ETL更新时间') COMMENT '兴证资管自TA资产账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1702$_tas_seller_no` as seller_no,
   `B$1702$_tas_dot_no` as dot_no,
   `B$1702$_tas_trans_acct` as trans_acct,
   `B$1702$_tas_trans_acct_stat` as trans_acct_stat,
   `B$1702$_tas_acct_default_dividend_mode` as acct_default_dividend_mode,
   `B$1702$_tas_bank_name` as bank_name,
   `B$1702$_tas_bank_acct` as bank_acct,
   `B$1702$_tas_bank_acct_name` as bank_acct_name,
   `B$1702$_tas_operation_dot_no` as operation_dot_no,
   `B$1702$_tas_trans_acct_bak` as trans_acct_bak,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1702';




 drop view  if exists ${hiveconf:namespace}.acc_ast_ds;
 create view if not exists ${hiveconf:namespace}.acc_ast_ds (
group_id comment '群组编号',
 src_cust_id comment '源系统客户号',
 cust_id comment '客户编号',
 acct_id comment '账户编号',
 acct_card comment '账户卡',
 acct_name comment '账户名称',
 main_acct_flag comment '主账户标志',
 src_sys comment '来源系统',
 upd_sys comment '更新系统',
 data_crt_tm comment '数据创建时间',
 data_upd_tm comment '数据更新时间',
 etl_crt_tm comment 'ETL创建时间',
 etl_upd_tm comment 'ETL更新时间') COMMENT '兴证资管直销资产账户'
 as
  select`group_id` as group_id,
   `src_cust_id` as src_cust_id,
   `cust_id` as cust_id,
   `acct_id` as acct_id,
   `B$1703$ds_acct_card` as acct_card,
   `B$1703$ds_acct_name` as acct_name,
   `B$1703$ds_main_acct_flag` as main_acct_flag,
   `src_sys` as src_sys,
   `upd_sys` as upd_sys,
   `data_crt_tm` as data_crt_tm,
   `data_upd_tm` as data_upd_tm,
   `etl_crt_tm` as etl_crt_tm,
   `etl_upd_tm` as etl_upd_tm
 from 
  ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1703';


 drop view  if exists ${hiveconf:namespace}.acc_ast_oth;
 create view if not exists ${hiveconf:namespace}.acc_ast_oth(
    group_id comment '群组编号',
    src_cust_id comment '源系统客户号',
    cust_id comment '客户编号',
    acct_id comment '账户编号',
    asset_acct comment '资产账户',
    branch_no comment '营业部号',
    fund_card comment '资金卡号',
    main_acct_flag comment '主账标志',
    risk_level comment '风险级别',
    risk_end_dt comment '风险截止日期',
    inter_asset_acct comment '内部资产账号',
    ust_grp comment '客户群组',
    room_code comment '客户分组',
    asset_prop comment '资产属性',
    fare_type_str comment '费用类型串',
    dist_model comment '折扣模板',
    perm_entrust_mode comment '允许委托模式',
    cust_rights comment '客户权限',
    cust_restriction comment '客户限制',
    profit_cal_mode comment '盈亏计算模式',
    product_flag comment '产品标志',
    limit_flag comment '限制标志',
    acct_stat comment '账户状态代码',
    open_dt comment '开户日期',
    cancel_dt comment '销户日期',
    src_sys comment '来源系统',
    upd_sys comment '更新系统',
    data_crt_tm comment '数据创建时间',
    data_upd_tm comment '数据更新时间',
    etl_crt_tm comment 'ETL创建时间',
    etl_upd_tm comment 'ETL更新时间'
 ) COMMENT '经纪业务补充账户'
 as
 select
    `group_id` as group_id,
    `src_cust_id` as src_cust_id,
    `cust_id` as cust_id,
    `acct_id` as acct_id,
    `acct_id` as asset_acct,
    `B$1099$_ord_acct_branch_no` as branch_no,
    `B$1099$_ord_acct_fund_card` as fund_card,
    `B$1099$_ord_acct_main_acct_flag` as main_acct_flag,
    `B$1099$_ord_acct_risk_level` as risk_level,
    `B$1099$_ord_acct_risk_end_dt` as risk_end_dt,
    `B$1099$_ord_acct_inter_asset_acct` as inter_asset_acct,
    `B$1099$_ord_acct_ust_grp` as ust_grp,
    `B$1099$_ord_acct_room_code` as room_code,
    `B$1099$_ord_acct_asset_prop` as asset_prop,
    `B$1099$_ord_acct_fare_type_str` as fare_type_str,
    `B$1099$_ord_acct_dist_model` as dist_model,
    `B$1099$_ord_acct_perm_entrust_mode` as perm_entrust_mode,
    `B$1099$_ord_acct_cust_rights` as cust_rights,
    `B$1099$_ord_acct_cust_restriction` as cust_restriction,
    `B$1099$_ord_acct_profit_cal_mode` as profit_cal_mode,
    `B$1099$_ord_acct_product_flag` as product_flag,
    `B$1099$_ord_acct_limit_flag` as limit_flag,
    `acct_stat_cd` as acct_stat,
    `open_dt` as open_dt,
    `cancel_dt` as cancel_dt,
    `src_sys` as src_sys,
    `upd_sys` as upd_sys,
    `data_crt_tm` as data_crt_tm,
    `data_upd_tm` as data_upd_tm,
    `etl_crt_tm` as etl_crt_tm,
    `etl_upd_tm` as etl_upd_tm
 from
     ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1099';


 drop view  if exists ${hiveconf:namespace}.acc_ast_ft;
 create view if not exists ${hiveconf:namespace}.acc_ast_ft(
    group_id comment '群组编号',
    src_cust_id comment '源系统客户号',
    cust_id comment '客户编号',
    acct_id comment '账户编号',
    asset_acct comment '资产账户',
    branch_no comment '营业部号',
    fund_card comment '资金卡号',
    main_flag comment '主账标志',
    risk_level comment '风险级别',
    risk_end_date comment '风险截止日期',
    inter_asset_acct comment '内部资产账号',
    client_group comment '客户群组',
    room_code comment '客户分组',
    asset_prop comment '资产属性',
    fare_kind_str comment '费用类型串',
    discount_model comment '折扣模板',
    en_entrust_way comment '允许委托模式',
    client_rights comment '客户权限',
    restriction comment '客户限制',
    profit_flag comment '盈亏计算模式',
    product_flag comment '产品标志',
    limit_flag comment '限制标志',
    acct_stat comment '账户状态代码',
    open_dt comment '开户日期',
    cancel_dt comment '销户日期',
    src_sys comment '来源系统',
    upd_sys comment '更新系统',
    data_crt_tm comment '数据创建时间',
    data_upd_tm comment '数据更新时间',
    etl_crt_tm comment 'ETL创建时间',
    etl_upd_tm comment 'ETL更新时间'
 ) COMMENT '经纪业务期货理财账户'
 as
 select
    `group_id` as group_id,
    `src_cust_id` as src_cust_id,
    `cust_id` as cust_id,
    `acct_id` as acct_id,
    `acct_id` as asset_acct,
    `B$1007$_ord_acct_branch_no` as branch_no,
    `B$1007$_ord_acct_fund_card` as fund_card,
    `B$1007$_ord_acct_main_acct_flag` as main_flag,
    `B$1007$_ord_acct_risk_level` as risk_level,
    `B$1007$_ord_acct_risk_end_dt` as risk_end_date,
    `B$1007$_ord_acct_inter_asset_acct` as inter_asset_acct,
    `B$1007$_ord_acct_ust_grp` as client_group,
    `B$1007$_ord_acct_room_code` as room_code,
    `B$1007$_ord_acct_asset_prop` as asset_prop,
    `B$1007$_ord_acct_fare_type_str` as fare_kind_str,
    `B$1007$_ord_acct_dist_model` as discount_model,
    `B$1007$_ord_acct_perm_entrust_mode` as en_entrust_way,
    `B$1007$_ord_acct_cust_rights` as client_rights,
    `B$1007$_ord_acct_cust_restriction` as restriction,
    `B$1007$_ord_acct_profit_cal_mode` as profit_flag,
    `B$1007$_ord_acct_product_flag` as product_flag,
    `B$1007$_ord_acct_limit_flag` as limit_flag,
    `acct_stat_cd` as acct_stat,
    `open_dt` as open_dt,
    `cancel_dt` as cancel_dt,
    `src_sys` as src_sys,
    `upd_sys` as upd_sys,
    `data_crt_tm` as data_crt_tm,
    `data_upd_tm` as data_upd_tm,
    `etl_crt_tm` as etl_crt_tm,
    `etl_upd_tm` as etl_upd_tm
 from
     ${hiveconf:namespace}.ext_rel_pty_acc_full where trim(acct_type_cd)='1007';