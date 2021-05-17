drop table if exists ${hiveconf:namespace}.PTY_INDV;
drop view if exists ${hiveconf:namespace}.PTY_INDV;
create view ${hiveconf:namespace}.PTY_INDV(
  `group_id`  comment '群组编号',
  `src_sys`  comment '来源系统',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `cert_no`  comment '证件号码',
  `cert_type_cd`  comment '证件类型',
  `cn_name`  comment '中文名称',
  `en_name`  comment '英文名称',
  `name_py`  comment '拼音名称',
  `used_name`  comment '曾用名称',
  `birth_dt`  comment '出生日期',
  `birth_country`  comment '出生国家',
  `province`  comment '省份',
  `city`  comment '城市',
  `ethnic`  comment '民族',
  `mother_lng`  comment '母语',
  `nationality`  comment '国籍',
  `native_place`  comment '籍贯',
  `residtype`  comment '居民类型代码',
  `domc_lo`  comment '户籍地',
  `gender`  comment '性别',
  `bldtp`  comment '血型',
  `cust_src`  comment '客户来源',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_sys` ,
  `src_cust_id` ,
  `cust_id` ,
  `cert_no` ,
  `cert_type_cd` ,
  `cn_name` ,
  `en_name` ,
  `name_py` ,
  `used_name` ,
  `birth_dt` ,
  `country` ,
  `province` ,
  `city` ,
  `ethnic` ,
  `prn_lng` ,
  `nationality` ,
  `native_place` ,
  `residtype` ,
  `domc_lo` ,
  `gender` ,
  `bldtp` ,
  `cust_src` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_INDV;


drop table if exists ${hiveconf:namespace}.PTY_INDV_SUB;
drop view if exists ${hiveconf:namespace}.PTY_INDV_SUB;
create view ${hiveconf:namespace}.PTY_INDV_SUB(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `lastname`  comment '姓',
  `firstname`  comment '名',
  `rlg_blf`  comment '宗教信仰',
  `polit_face`  comment '政治面貌',
  `height`  comment '身高',
  `weight`  comment '体重',
  `dead_dt`  comment '死亡日期',
  `drvlics_type_cd`  comment '驾照类型代码',
  `socseuy_flag`  comment '社保标志',
  `idv_socseuy_id`  comment '个人社保编号',
  `marr_stat_cd`  comment '婚姻状态代码',
  `gb_stat_cd`  comment '生育状态代码',
  `hlt_stat_cd`  comment '健康状态代码',
  `fa_mem_cnt`  comment '家庭成员数目',
  `is_have_child_flag`  comment '是否有子女标志',
  `child_cnt`  comment '子女个数',
  `is_have_vehic_flag`  comment '是否有车标志',
  `live_situ`  comment '居住状况',
  `is_local_estate_flag`  comment '是否本地房产标志',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `lastname` ,
  `firstname` ,
  `rlg_blf` ,
  `polit_face` ,
  `height` ,
  `weight` ,
  `dead_dt` ,
  `drvlics_type_cd` ,
  `socseuy_flag` ,
  `idv_socseuy_id` ,
  `marr_stat_cd` ,
  `gb_stat_cd` ,
  `hlt_stat_cd` ,
  `fa_mem_cnt` ,
  `is_have_child_flag` ,
  `child_cnt` ,
  `is_have_vehic_flag` ,
  `live_situ` ,
  `is_local_estate_flag` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_INDV_SUB;


drop table if exists ${hiveconf:namespace}.PTY_CORP;
drop view if exists ${hiveconf:namespace}.PTY_CORP;
create view ${hiveconf:namespace}.PTY_CORP(
  `group_id`  comment '群组编号',
  `src_sys`  comment '来源系统',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `cust_type_cd`  comment '客户类型代码',
  `org_inst_cd`  comment '组织机构代码',
  `biz_license_no`  comment '营业执照号',
  `un_soc_cr_cd`  comment '统一社会信用代码',
  `cn_name`  comment '中文名称',
  `prd_name`  comment '产品名称',
  `corp_type_cd`  comment '公司类型代码',
  `oprt_stat`  comment '经营状态',
  `oprt_scope`  comment '经营范围',
  `is_list_corp_flag`  comment '是否上市公司标志',
  `is_statown_flag`  comment '是否国有标志',
  `industry`  comment '行业',
  `corp_tel`  comment '公司电话',
  `lgl_represent`  comment '法人名称',
  `lgl_cert_no`  comment '法人证件号码',
  `lgl_cert_type_cd`  comment '法人证件类型代码',
  `rgst_cap`  comment '注册资本',
  `rgst_dt`  comment '注册日期',
  `corp_email`  comment '公司电子邮箱',
  `act_con_name`  comment '实际控制人名称',
  `act_con_cert_no`  comment '实际控制人证件号',
  `act_con_cert_type_cd`  comment '实际控制人证件类型',
  `rgst_addr`  comment '注册地址',
  `cont_name`  comment '联系人名称',
  `cont_tel`  comment '联系人电话',
  `his_name`  comment '曾用名',
  `cust_src`  comment '客户来源',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_sys` ,
  `src_cust_id` ,
  `cust_id` ,
  `cust_type_cd` ,
  `org_inst_cd` ,
  `biz_license_no` ,
  `un_soc_cr_cd` ,
  `cn_name` ,
  `prd_name` ,
  `corp_type_cd` ,
  `oprt_stat_cd` ,
  `oprt_scope` ,
  `is_list_corp_flag` ,
  `is_statown_flag` ,
  `industry` ,
  `corp_tel` ,
  `lgl_represent` ,
  `lgl_cert_no` ,
  `lgl_cert_type_cd` ,
  `rgst_cap` ,
  `rgst_dt` ,
  `corp_email` ,
  `act_con_name` ,
  `act_con_cert_no` ,
  `act_con_cert_type_cd` ,
  `rgst_addr` ,
  `cont_name` ,
  `cont_tel` ,
  `his_name` ,
  `cust_src` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_CORP;


drop table if exists ${hiveconf:namespace}.PTY_CORP_SUB;
drop view if exists ${hiveconf:namespace}.PTY_CORP_SUB;
create view ${hiveconf:namespace}.PTY_CORP_SUB(
  `cust_id`  comment '客户编号',
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源系统客户编号',
  `pripid`  comment '主体身份代码',
  `inv_name`  comment '投资人名称',
  `inv_type_cd`  comment '投资人类型代码',
  `inv_cert_type_cd`  comment '投资人证照类型代码',
  `subs_cap`  comment '认缴资本金',
  `paid_cap`  comment '实缴资本金',
  `fnd_dt`  comment '出资日期',
  `s_ext_timestamp`  comment '数据更新时间',
  `currency_cd`  comment '币种代码',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `cust_id` ,
  `group_id` ,
  `src_cust_id` ,
  `pripid` ,
  `inv_name` ,
  `inv_type_cd` ,
  `inv_cert_type_cd` ,
  `subs_cap` ,
  `paid_cap` ,
  `fnd_dt` ,
  `s_ext_timestamp` ,
  `currency_cd` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_CORP_SUB;


drop table if exists ${hiveconf:namespace}.PTY_CERT;
drop view if exists ${hiveconf:namespace}.PTY_CERT;
create view ${hiveconf:namespace}.PTY_CERT(
  `group_id`  comment '群组编号',
  `src_sys`  comment '来源系统',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `cn_name`  comment '中文名称',
  `cert_type_cd`  comment '证件类型代码',
  `cert_no`  comment '证件号码',
  `cert_valid_term`  comment '证件有效期',
  `cert_effect_dt`  comment '证件有效开始日期',
  `cert_end_dt`  comment '证件有效结束日期',
  `issue_org`  comment '证件签发机关',
  `cust_type_cd`  comment '客户类型代码',
  `is_idcard_flag`  comment '合法证件标志',
  `issue_dt`  comment '发证日期',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_sys` ,
  `src_cust_id` ,
  `cust_id` ,
  `cn_name` ,
  `cert_type_cd` ,
  `cert_no` ,
  `cert_valid_term` ,
  `cert_effect_dt` ,
  `cert_end_dt` ,
  `issue_org` ,
  `cust_type_cd` ,
  `is_idcard_flag` ,
  `issue_dt` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm`
from
${hiveconf:namespace}.EXT_PTY_CUST_INFO;


drop table if exists ${hiveconf:namespace}.PTY_TEL;
drop view if exists ${hiveconf:namespace}.PTY_TEL;
create view ${hiveconf:namespace}.PTY_TEL(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `cust_type_cd`  comment '客户类型代码',
  `tel_type_cd`  comment '电话类型代码',
  `country_no`  comment '国家号',
  `tel_areacode`  comment '电话区号',
  `tel_num`  comment '电话号码',
  `tel_ext`  comment '电话分机',
  `full_tel_num`  comment '完整电话号码',
  `succ_contact_flag`  comment '成功接触标志',
  `is_valid_flag`  comment '是否有效标志',
  `main_tel_flag`  comment '主电话标志',
  `operator_veri_flag`  comment '运营商校验标志',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `cust_type_cd` ,
  `tel_type_cd` ,
  `country_no` ,
  `tel_areacode` ,
  `tel_num` ,
  `tel_ext` ,
  `full_tel_num` ,
  `succ_contact_flag` ,
  `is_valid_flag` ,
  `main_tel_flag` ,
  `operator_veri_flag` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_TEL;


drop table if exists ${hiveconf:namespace}.PTY_ADDR;
drop view if exists ${hiveconf:namespace}.PTY_ADDR;
create view ${hiveconf:namespace}.PTY_ADDR(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `cust_type_cd`  comment '客户类型代码',
  `addr_type_cd`  comment '地址类型代码',
  `country`  comment '国家',
  `province`  comment '省',
  `city`  comment '市县',
  `district`  comment '区镇',
  `street`  comment '街道',
  `building`  comment '建筑物',
  `house_no`  comment '门牌号',
  `room_no`  comment '房号',
  `addr`  comment '地址',
  `zip_code`  comment '邮政编码',
  `valid_flag`  comment '有效标志',
  `main_addr_flag`  comment '主地址标志',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `cust_type_cd` ,
  `addr_type_cd` ,
  `country` ,
  `province` ,
  `city` ,
  `district` ,
  `street` ,
  `building` ,
  `house_no` ,
  `room_no` ,
  `addr` ,
  `zip_code` ,
  `valid_flag` ,
  `main_addr_flag` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_ADDR;


drop table if exists ${hiveconf:namespace}.PTY_NET_ADDR;
drop view if exists ${hiveconf:namespace}.PTY_NET_ADDR;
create view ${hiveconf:namespace}.PTY_NET_ADDR(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `cust_type_cd`  comment '客户类型代码',
  `net_addr_type_cd`  comment '互联网地址类型代码',
  `net_addr_info`  comment '互联网地址信息',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `cust_type_cd` ,
  `net_addr_type_cd` ,
  `net_addr_info` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_NET_ADDR;


drop table if exists ${hiveconf:namespace}.PTY_JOB;
drop view if exists ${hiveconf:namespace}.PTY_JOB;
create view ${hiveconf:namespace}.PTY_JOB(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `employ_stat_cd`  comment '就业状态代码',
  `industry`  comment '行业',
  `pztion`  comment '职位',
  `raks`  comment '职级',
  `title`  comment '职称',
  `job`  comment '职业',
  `parttime_job`  comment '兼职职业',
  `attend_work_dt`  comment '参加工作日期',
  `expect_retire_age`  comment '期望退休年龄',
  `unit_name`  comment '单位名称',
  `dept`  comment '部门',
  `emp_flag`  comment '员工标志',
  `se_flag`  comment '高管标志',
  `sharehd_flag`  comment '股东标志',
  `fin_cont_flag`  comment '财务联系人标志',
  `deci_flag`  comment '决策人标志',
  `emp_type_flag`  comment '员工类别代码',
  `is_prep_retire`  comment '待退休标志',
  `quit_dt`  comment '离职日期',
  `job_risk_lvl`  comment '职业风险等级',
  `retire_dt`  comment '退休日期',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `employ_stat_cd` ,
  `industry` ,
  `pztion` ,
  `raks` ,
  `title` ,
  `job` ,
  `parttime_job` ,
  `attend_work_dt` ,
  `expect_retire_age` ,
  `unit_name` ,
  `dept` ,
  `emp_flag` ,
  `se_flag` ,
  `sharehd_flag` ,
  `fin_cont_flag` ,
  `deci_flag` ,
  `emp_type_flag` ,
  `is_prep_retire` ,
  `quit_dt` ,
  `job_risk_lvl` ,
  `retire_dt` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_JOB;


drop table if exists ${hiveconf:namespace}.PTY_EDU;
drop view if exists ${hiveconf:namespace}.PTY_EDU;
create view ${hiveconf:namespace}.PTY_EDU(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `edu`  comment '学历',
  `major`  comment '专业',
  `college`  comment '院校',
  `degree`  comment '学位',
  `class_name`  comment '班级名称',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `edu` ,
  `major` ,
  `college` ,
  `degree` ,
  `class_name` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_EDU;


drop table if exists ${hiveconf:namespace}.PTY_PERFER;
drop view if exists ${hiveconf:namespace}.PTY_PERFER;
create view ${hiveconf:namespace}.PTY_PERFER(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源客户编号',
  `cust_id`  comment '客户编号',
  `prefer_cont_mode`  comment '偏好联系方式',
  `work_cont_dt`  comment '工作联系日期',
  `work_cont_tm`  comment '工作联系时间',
  `unwork_cont_dt`  comment '非工作日联系日期',
  `unwork_cont_tm`  comment '非工作日联系时间',
  `pref_lng`  comment '偏好语言',
  `hobby`  comment '爱好',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `prefer_cont_mode` ,
  `work_cont_dt` ,
  `work_cont_tm` ,
  `unwork_cont_dt` ,
  `unwork_cont_tm` ,
  `pref_lng` ,
  `hobby` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_PERFER;


drop table if exists ${hiveconf:namespace}.PTY_BANKACCT;
drop view if exists ${hiveconf:namespace}.PTY_BANKACCT;
create view ${hiveconf:namespace}.PTY_BANKACCT(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源客户编号',
  `cust_id`  comment '客户编号',
  `cust_type_cd`  comment '客户类型代码',
  `bank_acct`  comment '银行账号',
  `open_dt`  comment '开户日期',
  `bank_cd`  comment '银行代码',
  `bank_name`  comment '银行名称',
  `acct_stat`  comment '账户状态',
  `acct_usage`  comment '账户用途',
  `currency_cd`  comment '币种代码',
  `bank_acct_name`  comment '银行账户名称',
  `acct_valid_dt`  comment '账户有效日期',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `cust_type_cd` ,
  `bank_acct` ,
  `open_dt` ,
  `bank_cd` ,
  `bank_name` ,
  `acct_stat` ,
  `acct_usage` ,
  `currency_cd` ,
  `bank_acct_name` ,
  `acct_valid_dt` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_BANKACCT;


drop table if exists ${hiveconf:namespace}.PTY_INCOME_EXPNS;
drop view if exists ${hiveconf:namespace}.PTY_INCOME_EXPNS;
create view ${hiveconf:namespace}.PTY_INCOME_EXPNS(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `y_income`  comment '年收入',
  `m_income`  comment '月收入',
  `last_year_salary`  comment '上个年度工资',
  `currency_cd`  comment '币种代码',
  `income_src`  comment '收入来源',
  `mate_m_income`  comment '配偶月收入',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `y_income` ,
  `m_income` ,
  `last_year_salary` ,
  `currency_cd` ,
  `income_src` ,
  `mate_m_income` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_INCOME_EXPNS;


drop table if exists ${hiveconf:namespace}.PTY_CONT;
drop view if exists ${hiveconf:namespace}.PTY_CONT;
create view ${hiveconf:namespace}.PTY_CONT(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `rela_type_cd`  comment '关联人类型代码',
  `rela_name`  comment '关联人名称',
  `rela_phone_no`  comment '关联人手机号',
  `rela_cert_type_cd`  comment '关联人证件类型代码',
  `rela_cert_no`  comment '关联人证件号',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `rela_type_cd` ,
  `rela_name` ,
  `rela_phone_no` ,
  `rela_cert_type_cd` ,
  `rela_cert_no` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_CONT;


drop table if exists ${hiveconf:namespace}.REL_FAMILY_MEM;
drop view if exists ${hiveconf:namespace}.REL_FAMILY_MEM;
create view ${hiveconf:namespace}.REL_FAMILY_MEM(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `fam_id`  comment '家庭编号',
  `mem_id`  comment '成员编号',
  `role`  comment '角色',
  `name`  comment '名称',
  `cert_no`  comment '证件号码',
  `cert_type_cd`  comment '证件类型代码',
  `age`  comment '年龄',
  `gender`  comment '性别',
  `tel`  comment '电话号码',
  `company`  comment '公司',
  `cust_flag`  comment '客户标志',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `fam_id` ,
  `mem_id` ,
  `role` ,
  `name` ,
  `cert_no` ,
  `cert_type_cd` ,
  `age` ,
  `gender` ,
  `tel` ,
  `company` ,
  `cust_flag` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_REL_FAMILY_MEM;


drop table if exists ${hiveconf:namespace}.REL_CUST_ORG;
drop view if exists ${hiveconf:namespace}.REL_CUST_ORG;
create view ${hiveconf:namespace}.REL_CUST_ORG(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `org_id`  comment '机构编号',
  `org_lvl_cd`  comment '机构级别代码',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `org_id` ,
  `org_lvl_cd` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_REL_CUST_ORG;


drop table if exists ${hiveconf:namespace}.PTY_STOCK;
drop view if exists ${hiveconf:namespace}.PTY_STOCK;
create view ${hiveconf:namespace}.PTY_STOCK(
  `group_id`  comment '群组编号',
  `src_cust_id`  comment '源系统客户编号',
  `cust_id`  comment '客户编号',
  `cust_type_cd`  comment '客户类型代码',
  `un_soc_cr_cd`  comment '统一社会信用代码',
  `stock_cd`  comment '股票代码',
  `stock_name`  comment '股票名称',
  `stock_type_cd`  comment '股票类型代码',
  `cn_name`  comment '中文名称',
  `src_sys`  comment '来源系统',
  `upd_sys`  comment '更新系统',
  `data_crt_tm`  comment '数据创建时间',
  `data_upd_tm`  comment '数据更新时间',
  `etl_crt_tm`  comment 'ETL创建时间',
  `etl_upd_tm`  comment 'ETL更新时间'
)
as select
  `group_id` ,
  `src_cust_id` ,
  `cust_id` ,
  `cust_type_cd` ,
  `un_soc_cr_cd` ,
  `stock_cd` ,
  `stock_name` ,
  `stock_type_cd` ,
  `cn_name` ,
  `src_sys` ,
  `upd_sys` ,
  `data_crt_tm` ,
  `data_upd_tm` ,
  `etl_crt_tm` ,
  `etl_upd_tm` 
from
${hiveconf:namespace}.EXT_PTY_STOCK;

drop table if exists ${hiveconf:namespace}.PTY_MAIN_INFO;
drop view if exists ${hiveconf:namespace}.PTY_MAIN_INFO;
create view ${hiveconf:namespace}.PTY_MAIN_INFO(
                                                `cust_id`  comment '客户编号',
                                                `cn_name`  comment '中文名称',
                                                `nationality`  comment '国籍',
                                                `ethnic`  comment '民族',
                                                `city`  comment '城市',
                                                `native_place`  comment '籍贯',
                                                `domc_lo`  comment '户籍地',
                                                `gender`  comment '性别',
                                                `birth_dt`  comment '出生日期',
                                                `rlg_blf`  comment '宗教信仰',
                                                `polit_face`  comment '政治面貌',
                                                `marr_stat_cd`  comment '婚姻状态代码',
                                                `gb_stat_cd`  comment '生育状态代码',
                                                `hlt_stat_cd`  comment '健康状态代码',
                                                `corp_type_cd`  comment '公司类型代码',
                                                `industry`  comment '行业',
                                                `oprt_stat`  comment '经营状态',
                                                `oprt_scope`  comment '经营范围',
                                                `is_statown_flag`  comment '是否国有标志',
                                                `rgst_cap`  comment '注册资本',
                                                `rgst_dt`  comment '注册日期',
                                                `corp_tel`  comment '公司电话',
                                                `corp_email`  comment '公司电子邮箱',
                                                `is_list_corp_flag`  comment '是否上市公司标志',
                                                `stock_cd`  comment '股票代码',
                                                `stock_name`  comment '股票名称',
                                                `lgl_represent`  comment '法人名称',
                                                `lgl_cert_no`  comment '法人证件号码',
                                                `lgl_cert_type_cd`  comment '法人证件类型代码',
                                                `act_con_name`  comment '实际控制人名称',
                                                `act_con_cert_no`  comment '实际控制人证件号',
                                                `act_con_cert_type_cd`  comment '实际控制人证件类型',
                                                 `his_name`  comment '曾用名',
                                                `org_inst_cd`  comment '组织机构代码',
                                                `biz_license_no`  comment '营业执照',
                                                `un_soc_cr_cd`  comment '统一社会信用代码'
    )
as select
       `cust_id` ,
       `cn_name` ,
       `nationality` ,
       `ethnic` ,
       `city` ,
       `native_place` ,
       `domc_lo` ,
       `gender` ,
       `birth_dt` ,
       `rlg_blf` ,
       `polit_face` ,
       `marr_stat_cd` ,
       `gb_stat_cd` ,
       `hlt_stat_cd` ,
       `corp_type_cd` ,
       `industry` ,
       `oprt_stat_cd` ,
       `oprt_scope` ,
       `is_statown_flag` ,
       `rgst_cap` ,
       `rgst_dt` ,
       `corp_tel` ,
       `corp_email` ,
       `is_list_corp_flag` ,
       `stock_cd` ,
       `stock_name` ,
       `lgl_represent` ,
       `lgl_cert_no` ,
       `lgl_cert_type_cd` ,
       `act_con_name` ,
       `act_con_cert_no` ,
       `act_con_cert_type_cd`,
       `his_name`,
       `org_inst_cd` ,
       `biz_license_no` ,
       `un_soc_cr_cd`
     from
       ${hiveconf:namespace}.EXT_PTY_MAIN_INFO;


drop table if exists ${hiveconf:namespace}.PTY_AML_RISK;
drop view if exists ${hiveconf:namespace}.PTY_AML_RISK;
create view ${hiveconf:namespace}.PTY_AML_RISK(
                `group_id`  comment '群组编号',
                `src_cust_id`  comment '源系统客户编号',
                `cust_id`  comment '客户编号',
                `cust_type_cd`  comment '客户类型代码',
                `aml_risk_level`  comment '反洗钱风险等级',
                `aml_risk_score`  comment '反洗钱等级分数',
                `aml_update_time`  comment '反洗钱更新时间',
                `src_sys`  comment '来源系统',
                `upd_sys`  comment '更新系统',
                `data_crt_tm`  comment '数据创建时间',
                `data_upd_tm`  comment '数据更新时间',
                `etl_crt_tm`  comment 'ETL创建时间',
                `etl_upd_tm`  comment 'ETL更新时间'
    )
as select
      `group_id` ,
      `src_cust_id` ,
      `cust_id` ,
      `cust_type_cd` ,
      `aml_risk_level` ,
      `aml_risk_score` ,
      `aml_update_time` ,
      `src_sys` ,
      `upd_sys` ,
      `data_crt_tm` ,
      `data_upd_tm` ,
      `etl_crt_tm` ,
      `etl_upd_tm`
   from
       ${hiveconf:namespace}.EXT_PTY_AML_RISK;