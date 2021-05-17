--S001
-- drop table if exists ecifdb.o_s001_cust_info;
-- create table ecifdb.o_s001_cust_info as
-- select t1.c_custname as name, t1.*
-- from ecifdb.o_s001_tas_tcustomerinfo t1
-- where t1.c_custtype <> '2'
-- union all
-- select nvl(t2.entname, t1.c_custname) as name, t1.*
-- from ecifdb.o_s001_tas_tcustomerinfo t1
--          left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.c_identityno = t2.regno and t2.regno <> ''
-- where t1.c_custtype = '2'
--   and t1.c_identitytype = '1' -- 关联营业执照
-- union all
-- select nvl(t2.entname, t1.c_custname) as name, t1.*
-- from ecifdb.o_s001_tas_tcustomerinfo t1
--          left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.c_identityno = t2.org_inst_cd and t2.org_inst_cd <> ''
-- where t1.c_custtype = '2'
--   and t1.c_identitytype = '0' -- 组织机构代码
-- union all
-- select t1.c_custname as name, t1.*
-- from ecifdb.o_s001_tas_tcustomerinfo t1
-- where t1.c_custtype = '2'
--   and t1.c_identitytype not in ('1', '2');

--S002
-- drop table if exists ecifdb.o_s002_cust_info;
-- create table ecifdb.o_s002_cust_info as
-- select t1.c_custname as name, t1.*
-- from ecifdb.o_s002_stas_tcustinfo t1
-- where t1.c_custtype <> '2'
-- union all
-- select nvl(t2.entname, t1.c_custname) as name, t1.*
-- from ecifdb.o_s002_stas_tcustinfo t1
--          left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.c_identityno = t2.regno and t2.regno <> ''
-- where t1.c_custtype = '2'
--   and t1.c_identitytype = '1'
-- union all
-- select nvl(t2.entname, t1.c_custname) as name, t1.*
-- from ecifdb.o_s002_stas_tcustinfo t1
--          left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.c_identityno = t2.org_inst_cd and t2.org_inst_cd <> ''
-- where t1.c_custtype = '2'
--   and t1.c_identitytype = '0' -- 组织机构代码
-- union all
-- select t1.c_custname as name, t1.*
-- from ecifdb.o_s002_stas_tcustinfo t1
-- where t1.c_custtype = '2'
--   and t1.c_identitytype not in ('1', '2');

--S006
-- drop table if exists ecifdb.o_s006_cust_info;
-- create table ecifdb.o_s006_cust_info as
-- select t1.investorname as name, t1.*
-- from ecifdb.o_s006_qh_bs_customer t1
-- where t1.investortype not in ('2', '3', '4', '5', '6', '9')
-- union all
-- select case
--            when t3.entname <> '' then t3.entname --统一社会信用代码
--            when t4.entname <> '' then t4.entname --组织机构代码
--            when t2.entname <> '' then t2.entname --营业执照
--            else t1.investorname
--            end as name,
--        t1.*
-- from ecifdb.o_s006_qh_bs_customer t1
--          left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.licenseno = t2.regno and t2.regno <> '' --营业执照
--          left join ecifdb.o_s099_zj_xyzq_ent_info t3 on t1.licenseno = t3.credit_code and t3.credit_code <> '' --统一社会信用代码
--          left join ecifdb.o_s099_zj_xyzq_ent_info t4 on t1.eid = t4.org_inst_cd and t4.org_inst_cd <> '' -- 组织机构代码
-- where t1.investortype in ('2', '3', '4', '5', '6', '9');


--S009
select "开始执行s009 client 表产品户清洗";
drop table if exists ecifdb.o_s009_cust_info;
create table ecifdb.o_s009_cust_info as
select case when t1.full_name = '' then t1.client_name else t1.full_name end as name, t1.*, null as lic, null as org_cd
from ecifdb.o_s009_oms_client t1
where t1.organ_flag <> '3'
union all
select case
           when t2.entname <> '' then t2.entname --统一社会信用代码
           when t5.entname <> '' then t5.entname --组织机构代码
           when t7.entname <> '' then t7.entname --组织机构代码
           when t6.entname <> '' then t6.entname --组织机构代码
           else case when t1.full_name = '' then t1.client_name else t1.full_name end end as name,
       t1.*
from (select t3.*,
             case
                 when t4.business_licence <> '' then business_licence
                 when t3.id_kind = '2' and t3.id_no <> '' then t3.id_no
                 else '' end as lic,       --营业执照， 里面可能包含营业执照，组织机构代码，统一社会信用代码
             case
                 when t4.organ_code <> '' then t4.organ_code
                 when t3.id_kind = 'P' and t3.id_no <> '' then t3.id_no
                 else '' end as org_cd -- 组织机构代码
      from ecifdb.o_s009_oms_client t3
               left join ecifdb.o_s009_oms_organinfo t4 on t3.client_id = t4.client_id) t1
         left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.lic = t2.credit_code and t2.credit_code <> '' --统一社会信用代码
         left join ecifdb.o_s099_zj_xyzq_ent_info t5 on t1.lic = t5.org_inst_cd and t5.org_inst_cd <> '' --组织机构代码
         left join ecifdb.o_s099_zj_xyzq_ent_info t6 on t1.lic = t6.regno and t6.regno <> '' --营业执照
         left join ecifdb.o_s099_zj_xyzq_ent_info t7 on t1.org_cd = t7.org_inst_cd and t7.org_inst_cd <> '' --组织机构代码
where t1.organ_flag = '3'
  and t1.id_kind = '2'
union all
select case when t1.full_name = '' then t1.client_name else t1.full_name end as name,
       t1.*,
       null                                                                  as lic,
       null                                                                  as org_cd
from ecifdb.o_s009_oms_client t1
where t1.organ_flag = '3'
  and t1.id_kind <> '2';
select "s009 client 表产品户清洗完成";
