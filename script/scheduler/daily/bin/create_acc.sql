DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_INFO;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_INFO(
                                                           pbk_id string comment "分区业务编号",
                                                           bk_id string comment "业务编号",
                                                           row_lock int comment "行级锁",
                                                           group_id string comment "群组编号",
                                                           src_cust_id string comment "源系统客户号",
                                                           cust_id string comment "客户编号",
                                                           acct_id string comment "账户编号",
                                                           acct_stat_cd string comment "账户状态代码",
                                                           open_dt date comment "开户日期",
                                                           cancel_dt date comment "销户日期",
                                                           bk_type_cd string comment "业务类别代码",
                                                           acct_type_cd string comment "账户类型",
                                                           acct_flag_cd string comment "账户标志",
                                                           cust_type_cd string comment "客户类型代码",
                                                           src_sys string comment "来源系统",
                                                           upd_sys string comment "更新系统",
                                                           data_crt_tm timestamp comment "数据创建时间",
                                                           data_upd_tm timestamp comment "数据更新时间",
                                                           etl_crt_tm timestamp comment "ETL创建时间",
                                                           etl_upd_tm timestamp comment "ETL更新时间",
                                                           is_merge_flag string comment "是否合并标志",
                                                           last_merge_rowkey string comment "上一次合并前行键"
)
  COMMENT '账户总表'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_BANK_FUTS;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_BANK_FUTS(
                                                                pbk_id string comment "分区业务编号",
                                                                bk_id string comment "业务编号",
                                                                row_lock int comment "行级锁",
                                                                group_id string comment "群组编号",
                                                                src_cust_id string comment "源系统客户号",
                                                                cust_id string comment "客户编号",
                                                                acct_id string comment "账户编号",
                                                                bank_no string comment "银行代码",
                                                                currency_cd string comment "币种代码",
                                                                asset_acct string comment "资产账户",
                                                                bank_acct_flag string comment "银行账户标志",
                                                                bank_open_name string comment "银行开户名称",
                                                                bank_open_bank string comment "银行开户银行",
                                                                futs_settle_acct_type_cd string comment "结算账户类型代码",
                                                                src_sys string comment "来源系统",
                                                                upd_sys string comment "更新系统",
                                                                data_crt_tm timestamp comment "数据创建时间",
                                                                data_upd_tm timestamp comment "数据更新时间",
                                                                etl_crt_tm timestamp comment "ETL创建时间",
                                                                etl_upd_tm timestamp comment "ETL更新时间",
                                                                is_merge_flag string comment "是否合并标志",
                                                                last_merge_rowkey string comment "上一次合并前行键"
)
  COMMENT '兴证期货结算账户'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_BANK_TPD;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_BANK_TPD(
                                                               pbk_id string comment "分区业务编号",
                                                               bk_id string comment "业务编号",
                                                               row_lock int comment "行级锁",
                                                               group_id string comment "群组编号",
                                                               src_cust_id string comment "源系统客户号",
                                                               cust_id string comment "客户编号",
                                                               acct_id string comment "账户编号",
                                                               bank_no string comment "银行代码",
                                                               currency_cd string comment "币种代码",
                                                               asset_acct string comment "资产账户",
                                                               bank_acct_type_cd string comment "银行帐号类别代码",
                                                               main_acct_flag string comment "主账户标志",
                                                               branch_no string comment "营业部号",
                                                               cust_acct string comment "客户账户",
                                                               holder_name string comment "股东名称",
                                                               foreign_flag string comment "境外标志",
                                                               cert_type_cd string comment "证件类别代码",
                                                               cert_no string comment "证件号码",
                                                               auth_no int comment "自有号",
                                                               out_amt_upper_limit decimal(19,2) comment "转出金额上限",
  out_amt_lower_limit decimal(19,2) comment "转出金额下限",
  in_amt_upper_limit decimal(19,2) comment "转进金额上限",
  in_amt_lower_limit decimal(19,2) comment "转进金额下限",
  con_tpl int comment "控制模板",
  bank_operator string comment "银行操作员",
  bank_appt_flag string comment "存管指定标志",
  perm_entrust_mode string comment "允许委托方式",
  bank_acct_right string comment "银行账户权限",
  bank_acct_lmt string comment "银行账户限制",
  bank_book_acct string comment "银行簿记账户",
  prov_branch_cd string comment "省分行代码",
  city_branch_cd string comment "市分行代码",
  county_branch_cd string comment "县分行代码",
  sub_branch_cd string comment "子代码",
  posit_str string comment "定位串",
  remark string comment "备注",
  src_sys string comment "来源系统",
  upd_sys string comment "更新系统",
  data_crt_tm timestamp comment "数据创建时间",
  data_upd_tm timestamp comment "数据更新时间",
  etl_crt_tm timestamp comment "ETL创建时间",
  etl_upd_tm timestamp comment "ETL更新时间",
  is_merge_flag string comment "是否合并标志",
  last_merge_rowkey string comment "上一次合并前行键"
  )
  COMMENT '经纪业务三方存管账户'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_AST_FUTSO;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_AST_FUTSO(
                                                                pbk_id string comment "分区业务编号",
                                                                bk_id string comment "业务编号",
                                                                row_lock int comment "行级锁",
                                                                group_id string comment "群组编号",
                                                                src_cust_id string comment "源系统客户号",
                                                                cust_id string comment "客户编号",
                                                                acct_id string comment "账户编号",
                                                                exchange_flag string comment "交易所标志",
                                                                freeze_stat string comment "休眠状态",
                                                                src_sys string comment "来源系统",
                                                                upd_sys string comment "更新系统",
                                                                data_crt_tm timestamp comment "数据创建时间",
                                                                data_upd_tm timestamp comment "数据更新时间",
                                                                etl_crt_tm timestamp comment "ETL创建时间",
                                                                etl_upd_tm timestamp comment "ETL更新时间",
                                                                is_merge_flag string comment "是否合并标志",
                                                                last_merge_rowkey string comment "上一次合并前行键"
)
  COMMENT '兴证期货期权期货账户'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_AST_FUTSX;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_AST_FUTSX(
                                                                pbk_id string comment "分区业务编号",
                                                                bk_id string comment "业务编号",
                                                                row_lock int comment "行级锁",
                                                                group_id string comment "群组编号",
                                                                src_cust_id string comment "源系统客户号",
                                                                cust_id string comment "客户编号",
                                                                acct_id string comment "账户编号",
                                                                exchange_flag string comment "交易所标志",
                                                                src_sys string comment "来源系统",
                                                                upd_sys string comment "更新系统",
                                                                data_crt_tm timestamp comment "数据创建时间",
                                                                data_upd_tm timestamp comment "数据更新时间",
                                                                etl_crt_tm timestamp comment "ETL创建时间",
                                                                etl_upd_tm timestamp comment "ETL更新时间",
                                                                is_merge_flag string comment "是否合并标志",
                                                                last_merge_rowkey string comment "上一次合并前行键"
)
  COMMENT '兴证期货现货账户'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_AST_ORD;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_AST_ORD(
                                                              pbk_id string comment "分区业务编号",
                                                              bk_id string comment "业务编号",
                                                              row_lock int comment "行级锁",
                                                              group_id string comment "群组编号",
                                                              src_cust_id string comment "源系统客户号",
                                                              cust_id string comment "客户编号",
                                                              acct_id string comment "账户编号",
                                                              branch_no string comment "营业部号",
                                                              fund_card string comment "资金卡",
                                                              main_acct_flag string comment "主账户标志",
                                                              risk_level int comment "风险级别",
                                                              risk_end_dt date comment "风险截止日期",
                                                              inter_asset_acct string comment "内部资产账户",
                                                              cust_grp string comment "客户群组",
                                                              room_divide string comment "客户分组",
                                                              asset_prop string comment "资产属性",
                                                              fare_type_str string comment "费用类型串",
                                                              dist_tpl int comment "折扣模板",
                                                              perm_entrust_mode string comment "允许委托模式",
                                                              cust_rights string comment "客户权限",
                                                              cust_restriction string comment "客户限制",
                                                              profit_cal_mode string comment "盈亏计算模式",
                                                              product_flag string comment "产品标志",
                                                              limit_flag int comment "限制标志",
                                                              src_sys string comment "来源系统",
                                                              upd_sys string comment "更新系统",
                                                              data_crt_tm timestamp comment "数据创建时间",
                                                              data_upd_tm timestamp comment "数据更新时间",
                                                              etl_crt_tm timestamp comment "ETL创建时间",
                                                              etl_upd_tm timestamp comment "ETL更新时间",
                                                              is_merge_flag string comment "是否合并标志",
                                                              last_merge_rowkey string comment "上一次合并前行键"
)
  COMMENT '经纪业务普通账户'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_AST_MT;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_AST_MT(
                                                             pbk_id string comment "分区业务编号",
                                                             bk_id string comment "业务编号",
                                                             row_lock int comment "行级锁",
                                                             group_id string comment "群组编号",
                                                             src_cust_id string comment "源系统客户号",
                                                             cust_id string comment "客户编号",
                                                             acct_id string comment "账户编号",
                                                             asset_acct string comment "资产账户",
                                                             fi_quota decimal(19,2) comment "融资额度（元）",
  fs_quota decimal(19,2) comment "融券额度（元）",
  max_quota decimal(19,2) comment "最大额度（元）",
  fi_rate decimal(14,13) comment "融资利率（%）",
  fs_rate decimal(14,13) comment "融券利率（%）",
  ctr_begin_dt date comment "合同开始日期",
  ctr_end_dt date comment "合同截止日期",
  src_sys string comment "来源系统",
  upd_sys string comment "更新系统",
  data_crt_tm timestamp comment "数据创建时间",
  data_upd_tm timestamp comment "数据更新时间",
  etl_crt_tm timestamp comment "ETL创建时间",
  etl_upd_tm timestamp comment "ETL更新时间",
  is_merge_flag string comment "是否合并标志",
  last_merge_rowkey string comment "上一次合并前行键"
  )
  COMMENT '经纪业务融资融券账户'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_AST_ISO;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_AST_ISO(
                                                              pbk_id string comment "分区业务编号",
                                                              bk_id string comment "业务编号",
                                                              row_lock int comment "行级锁",
                                                              group_id string comment "群组编号",
                                                              src_cust_id string comment "源系统客户号",
                                                              cust_id string comment "客户编号",
                                                              acct_id string comment "账户编号",
                                                              asset_acct string comment "资产账户",
                                                              cust_opt_level string comment "客户期权等级",
                                                              power_hold_quota decimal(19,2) comment "权力持仓额度",
  sh_pur_quota decimal(19,2) comment "上海限购额度（元）",
  total_hold_quota decimal(19,2) comment "总持仓限额（元）",
  sh_pur_quota_expire_dt date comment "上海限购额度到期日期",
  td_sum_buy_quota decimal(19,2) comment "当日累计买入额度",
  src_sys string comment "来源系统",
  upd_sys string comment "更新系统",
  data_crt_tm timestamp comment "数据创建时间",
  data_upd_tm timestamp comment "数据更新时间",
  etl_crt_tm timestamp comment "ETL创建时间",
  etl_upd_tm timestamp comment "ETL更新时间",
  is_merge_flag string comment "是否合并标志",
  last_merge_rowkey string comment "上一次合并前行键"
  )
  COMMENT '经纪业务个股期权账户'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_AST_BF;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_AST_BF(
                                                             pbk_id string comment "分区业务编号",
                                                             bk_id string comment "业务编号",
                                                             row_lock int comment "行级锁",
                                                             group_id string comment "群组编号",
                                                             src_cust_id string comment "源系统客户号",
                                                             cust_id string comment "客户编号",
                                                             acct_id string comment "账户编号",
                                                             asset_acct string comment "资产账户",
                                                             prod_ta_no string comment "产品TA号",
                                                             bank_fina_acct string comment "银行理财账户",
                                                             branch_no string comment "营业部号",
                                                             cert_type_cd string comment "证件类别代码",
                                                             cert_no string comment "证件号码",
                                                             prod_acct_stat string comment "产品账户状态",
                                                             dividend_mode string comment "分红方式",
                                                             seat_no string comment "席位号",
                                                             posi_str string comment "定位串",
                                                             ta_risk_level string comment "TA风险级别",
                                                             risk_end_dt date comment "风险截止日期",
                                                             trans_acct string comment "交易帐号",
                                                             operator_name string comment "操作人名称",
                                                             agent_cert_type_cd string comment "经办人证件类型代码",
                                                             agent_cert_no string comment "经办人证件号码",
                                                             src_sys string comment "来源系统",
                                                             upd_sys string comment "更新系统",
                                                             data_crt_tm timestamp comment "数据创建时间",
                                                             data_upd_tm timestamp comment "数据更新时间",
                                                             etl_crt_tm timestamp comment "ETL创建时间",
                                                             etl_upd_tm timestamp comment "ETL更新时间",
                                                             is_merge_flag string comment "是否合并标志",
                                                             last_merge_rowkey string comment "上一次合并前行键"
)
  COMMENT '经纪业务银行理财账户'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_AST_SF;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_AST_SF(
                                                             pbk_id string comment "分区业务编号",
                                                             bk_id string comment "业务编号",
                                                             row_lock int comment "行级锁",
                                                             group_id string comment "群组编号",
                                                             src_cust_id string comment "源系统客户号",
                                                             cust_id string comment "客户编号",
                                                             acct_id string comment "账户编号",
                                                             asset_acct string comment "资产账户",
                                                             prod_ta_no string comment "产品TA编号",
                                                             shares_fina_acct string comment "证券理财账户",
                                                             branch_no string comment "营业部号",
                                                             cert_type_cd string comment "证件类别代码",
                                                             cert_no string comment "证件号码",
                                                             prod_holder_stat string comment "产品账户状态",
                                                             dividend_mode string comment "分红方式",
                                                             remark string comment "备注",
                                                             seat_no string comment "席位号",
                                                             export_dt date comment "导出日期",
                                                             posi_str string comment "定位串",
                                                             prod_holder_type_cd string comment "产品持有类型代码",
                                                             trans_acct string comment "交易帐号",
                                                             register_flag string comment "登记标志",
                                                             shares_fina_acct_right string comment "证券理财账户权限",
                                                             pay_acct string comment "支付账户",
                                                             pay_type_cd string comment "支付方式代码",
                                                             bank_cd string comment "银行代码",
                                                             src_sys string comment "来源系统",
                                                             upd_sys string comment "更新系统",
                                                             data_crt_tm timestamp comment "数据创建时间",
                                                             data_upd_tm timestamp comment "数据更新时间",
                                                             etl_crt_tm timestamp comment "ETL创建时间",
                                                             etl_upd_tm timestamp comment "ETL更新时间",
                                                             is_merge_flag string comment "是否合并标志",
                                                             last_merge_rowkey string comment "上一次合并前行键"
)
  COMMENT '经纪业务证券理财账户'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_AST_SCC;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_AST_SCC(
                                                              pbk_id string comment "分区业务编号",
                                                              bk_id string comment "业务编号",
                                                              row_lock int comment "行级锁",
                                                              group_id string comment "群组编号",
                                                              src_cust_id string comment "源系统客户号",
                                                              cust_id string comment "客户编号",
                                                              acct_id string comment "账户编号",
                                                              cust_group string comment "客户群组",
                                                              com_clf string comment "公司分类",
                                                              bk_clf int comment "业务分类",
                                                              acct_type_cd string comment "帐户类别代码",
                                                              main_acct string comment "主账号",
                                                              src_sys string comment "来源系统",
                                                              upd_sys string comment "更新系统",
                                                              data_crt_tm timestamp comment "数据创建时间",
                                                              data_upd_tm timestamp comment "数据更新时间",
                                                              etl_crt_tm timestamp comment "ETL创建时间",
                                                              etl_upd_tm timestamp comment "ETL更新时间",
                                                              is_merge_flag string comment "是否合并标志",
                                                              last_merge_rowkey string comment "上一次合并前行键"
)
  COMMENT '海交中心资产账户'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_AST_TG;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_AST_TG(
                                                             pbk_id string comment "分区业务编号",
                                                             bk_id string comment "业务编号",
                                                             row_lock int comment "行级锁",
                                                             group_id string comment "群组编号",
                                                             src_cust_id string comment "源系统客户号",
                                                             cust_id string comment "客户编号",
                                                             acct_id string comment "账户编号",
                                                             fund_acct_no string comment "资金账户号",
                                                             prod_id int comment "产品编号",
                                                             prod_cd string comment "产品代码",
                                                             prod_abbr string comment "产品简称",
                                                             fund_acct_type_cd string comment "资金账户类型代码",
                                                             prod_stat string comment "产品状态",
                                                             prod_est_dt date comment "产品成立日期",
                                                             src_sys string comment "来源系统",
                                                             upd_sys string comment "更新系统",
                                                             data_crt_tm timestamp comment "数据创建时间",
                                                             data_upd_tm timestamp comment "数据更新时间",
                                                             etl_crt_tm timestamp comment "ETL创建时间",
                                                             etl_upd_tm timestamp comment "ETL更新时间",
                                                             is_merge_flag string comment "是否合并标志",
                                                             last_merge_rowkey string comment "上一次合并前行键"
)
  COMMENT '托管业务资产账户'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_ATS_TAS;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_ATS_TAS(
                                                              pbk_id string comment "分区业务编号",
                                                              bk_id string comment "业务编号",
                                                              row_lock int comment "行级锁",
                                                              group_id string comment "群组编号",
                                                              src_cust_id string comment "源系统客户号",
                                                              cust_id string comment "客户编号",
                                                              acct_id string comment "账户编号",
                                                              seller_no string comment "销售商号",
                                                              dot_no string comment "网点号",
                                                              trans_acct string comment "交易账户",
                                                              open_dot_flag string comment "开户网点标志",
                                                              acct_default_dividend_mode string comment "账户默认分红方式",
                                                              bank_name string comment "银行名称",
                                                              bank_acct string comment "银行行账户",
                                                              bank_acct_name string comment "银行行账户名称",
                                                              operation_dot_no string comment "操作网点号",
                                                              trans_acct_bak string comment "交易账户备份",
                                                              special_acct_flag string comment "特殊账户标志",
                                                              special_acct_id string comment "特殊账户编号",
                                                              his_flag string comment "历史标志",
                                                              src_sys string comment "来源系统",
                                                              upd_sys string comment "更新系统",
                                                              data_crt_tm timestamp comment "数据创建时间",
                                                              data_upd_tm timestamp comment "数据更新时间",
                                                              etl_crt_tm timestamp comment "ETL创建时间",
                                                              etl_upd_tm timestamp comment "ETL更新时间",
                                                              is_merge_flag string comment "是否合并标志",
                                                              last_merge_rowkey string comment "上一次合并前行键"
)
  COMMENT '兴证资管分TA资产账户'
  ROW FORMAT DELIMITED;


DROP TABLE IF EXISTS ${hiveconf:namespace}.ACC_AST_STAS;
CREATE TABLE IF NOT EXISTS ${hiveconf:namespace}.ACC_AST_STAS(
                                                               pbk_id string comment "分区业务编号",
                                                               bk_id string comment "业务编号",
                                                               row_lock int comment "行级锁",
                                                               group_id string comment "群组编号",
                                                               src_cust_id string comment "源系统客户号",
                                                               cust_id string comment "客户编号",
                                                               acct_id string comment "账户编号",
                                                               seller_no string comment "销售商编号",
                                                               dot_no string comment "网点编号",
                                                               trans_acct string comment "交易账户",
                                                               trans_acct_stat string comment "交易账户状态",
                                                               acct_default_dividend_mode string comment "账户默认分红方式",
                                                               bank_name string comment "银行名称",
                                                               bank_acct string comment "银行账户",
                                                               bank_acct_name string comment "银行账户名称",
                                                               operation_dot_no string comment "操作网点编号",
                                                               trans_acct_bak string comment "交易账户备份",
                                                               src_sys string comment "来源系统",
                                                               upd_sys string comment "更新系统",
                                                               data_crt_tm timestamp comment "数据创建时间",
                                                               data_upd_tm timestamp comment "数据更新时间",
                                                               etl_crt_tm timestamp comment "ETL创建时间",
                                                               etl_upd_tm timestamp comment "ETL更新时间",
                                                               is_merge_flag string comment "是否合并标志",
                                                               last_merge_rowkey string comment "上一次合并前行键"
)
  COMMENT '兴证资管自TA资产账户'
  ROW FORMAT DELIMITED;
