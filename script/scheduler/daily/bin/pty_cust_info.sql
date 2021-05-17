drop view if exists ${hiveconf:namespace}.PTY_CUST_INFO_LAST;
create view  ${hiveconf:namespace}.PTY_CUST_INFO_LAST(
`src_cust_id`  comment '客户编号',
`src_sys` comment '源系统',
`repair_dt` comment '修复日期',
`is_repair_flag` comment '是否修复标志'
) COMMENT '历史未修复客户'
 as
  select
    `src_cust_id` as src_cust_id,
    `src_sys` as src_sys,
    `repair_dt` as repair_dt,
    `is_repair_flag` as is_repair_flag
 from ${hiveconf:active_ns}.EXT_PTY_CUST_INFO
 group by src_cust_id, src_sys, repair_dt,is_repair_flag;