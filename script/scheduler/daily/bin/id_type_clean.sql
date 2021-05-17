-------- S001 ---------
select "开始执行s001 证件类型清洗";
drop table if exists ecifdb.t_s001_tas_tcustomerinfo;
create table ecifdb.t_s001_tas_tcustomerinfo as
select t1.c_custname as name, t1.c_custno,t1.oc_date,t1.c_custtype,t1.c_custname,t1.c_shortname,t1.c_helpcode,t1.c_identitytype,t1.c_identityno,t1.c_zipcode,t1.c_address,t1.c_phone,t1.c_faxno,t1.c_mobileno,t1.c_email,t1.c_sex,t1.c_birthday,t1.c_vocation,t1.c_education,t1.c_income,t1.c_contact,t1.c_contype,t1.c_contno,t1.c_billsendflag,t1.c_callcenter,t1.c_internet,t1.c_secretcode,t1.c_nationality,t1.c_cityno,t1.c_lawname,t1.c_shacco,t1.c_szacco,t1.c_broker,t1.f_agio,t1.c_memo,t1.c_reserve,t1.c_corpname,t1.c_corptel,t1.c_specialcode,t1.c_actcode,t1.c_billsendpass,t1.c_addressinvalid,t1.d_appenddate,t1.d_backdate,t1.c_invalidaddress,t1.c_backreason,t1.c_modifyinfo,t1.c_recommender,t1.c_recommendertype,t1.d_idnovaliddate,t1.c_instrepridcode,t1.c_instrepridtype,t1.d_contidnovaliddate,t1.d_lawidnovaliddate
from ecifdb.o_s001_tas_tcustomerinfo t1
where t1.c_custtype = '1' -- 个人客户
union all
select case
           when t1.c_custtype <> '2' then t1.c_custname
           when t3.entname is not null then t3.entname
           when t2.entname is not null then t2.entname
           else t1.c_custname end     as name
     , t1.c_custno
     , t1.oc_date
     , t1.c_custtype
     , t1.c_custname
     , t1.c_shortname
     , t1.c_helpcode
     , case
           when t3.entname is not null then 'B'
           else t1.c_identitytype end as c_identitytype
     , t1.c_identityno
     , t1.c_zipcode
     , t1.c_address
     , t1.c_phone
     , t1.c_faxno
     , t1.c_mobileno
     , t1.c_email
     , t1.c_sex
     , t1.c_birthday
     , t1.c_vocation
     , t1.c_education
     , t1.c_income
     , t1.c_contact
     , t1.c_contype
     , t1.c_contno
     , t1.c_billsendflag
     , t1.c_callcenter
     , t1.c_internet
     , t1.c_secretcode
     , t1.c_nationality
     , t1.c_cityno
     , t1.c_lawname
     , t1.c_shacco
     , t1.c_szacco
     , t1.c_broker
     , t1.f_agio
     , t1.c_memo
     , t1.c_reserve
     , t1.c_corpname
     , t1.c_corptel
     , t1.c_specialcode
     , t1.c_actcode
     , t1.c_billsendpass
     , t1.c_addressinvalid
     , t1.d_appenddate
     , t1.d_backdate
     , t1.c_invalidaddress
     , t1.c_backreason
     , t1.c_modifyinfo
     , t1.c_recommender
     , t1.c_recommendertype
     , t1.d_idnovaliddate
     , t1.c_instrepridcode
     , t1.c_instrepridtype
     , t1.d_contidnovaliddate
     , t1.d_lawidnovaliddate
from ecifdb.o_s001_tas_tcustomerinfo t1
         left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.c_identityno = t2.regno and t2.regno <> '' -- 营业执照
         left join ecifdb.o_s099_zj_xyzq_ent_info t3
                   on t1.c_identityno = t3.credit_code and t3.credit_code <> '' -- 统一社会信用代码
where t1.c_custtype <> '1'
  and t1.c_identitytype = '1' -- 关联营业执照
union all
select nvl(t2.entname, t1.c_custname) as name, t1.c_custno,t1.oc_date,t1.c_custtype,t1.c_custname,t1.c_shortname,t1.c_helpcode,t1.c_identitytype,t1.c_identityno,t1.c_zipcode,t1.c_address,t1.c_phone,t1.c_faxno,t1.c_mobileno,t1.c_email,t1.c_sex,t1.c_birthday,t1.c_vocation,t1.c_education,t1.c_income,t1.c_contact,t1.c_contype,t1.c_contno,t1.c_billsendflag,t1.c_callcenter,t1.c_internet,t1.c_secretcode,t1.c_nationality,t1.c_cityno,t1.c_lawname,t1.c_shacco,t1.c_szacco,t1.c_broker,t1.f_agio,t1.c_memo,t1.c_reserve,t1.c_corpname,t1.c_corptel,t1.c_specialcode,t1.c_actcode,t1.c_billsendpass,t1.c_addressinvalid,t1.d_appenddate,t1.d_backdate,t1.c_invalidaddress,t1.c_backreason,t1.c_modifyinfo,t1.c_recommender,t1.c_recommendertype,t1.d_idnovaliddate,t1.c_instrepridcode,t1.c_instrepridtype,t1.d_contidnovaliddate,t1.d_lawidnovaliddate
from ecifdb.o_s001_tas_tcustomerinfo t1
         left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.c_identityno = t2.org_inst_cd and t2.org_inst_cd <> ''
where t1.c_custtype <> '1'
  and t1.c_identitytype = '0' -- 组织机构代码
union all
select t1.c_custname as name, t1.c_custno,t1.oc_date,t1.c_custtype,t1.c_custname,t1.c_shortname,t1.c_helpcode,t1.c_identitytype,t1.c_identityno,t1.c_zipcode,t1.c_address,t1.c_phone,t1.c_faxno,t1.c_mobileno,t1.c_email,t1.c_sex,t1.c_birthday,t1.c_vocation,t1.c_education,t1.c_income,t1.c_contact,t1.c_contype,t1.c_contno,t1.c_billsendflag,t1.c_callcenter,t1.c_internet,t1.c_secretcode,t1.c_nationality,t1.c_cityno,t1.c_lawname,t1.c_shacco,t1.c_szacco,t1.c_broker,t1.f_agio,t1.c_memo,t1.c_reserve,t1.c_corpname,t1.c_corptel,t1.c_specialcode,t1.c_actcode,t1.c_billsendpass,t1.c_addressinvalid,t1.d_appenddate,t1.d_backdate,t1.c_invalidaddress,t1.c_backreason,t1.c_modifyinfo,t1.c_recommender,t1.c_recommendertype,t1.d_idnovaliddate,t1.c_instrepridcode,t1.c_instrepridtype,t1.d_contidnovaliddate,t1.d_lawidnovaliddate
from ecifdb.o_s001_tas_tcustomerinfo t1
where t1.c_custtype <> '1'
  and t1.c_identitytype not in ('1', '2');
--其它

-------- S001 ---------


-------- S002 ---------
select "开始执行s002 证件类型清洗";
drop table if exists ecifdb.t_s002_stas_tcustinfo;
create table ecifdb.t_s002_stas_tcustinfo as
select t1.c_custname as name, t1.oc_date,t1.c_custno,t1.c_fundacco,t1.c_accounttype,t1.c_custtype,t1.c_custname,t1.c_shortname,t1.c_identitytype,t1.c_identityno,t1.d_idnovaliddate,t1.c_idcard18len,t1.d_opendate,t1.d_lastmodify,t1.c_accostatus,t1.l_changetime,t1.c_zipcode,t1.c_address,t1.c_phone,t1.c_faxno,t1.c_mobileno,t1.c_email,t1.c_sex,t1.c_birthday,t1.c_vocation,t1.c_education,t1.c_income,t1.c_corpname,t1.c_corptel,t1.c_contact,t1.c_contype,t1.c_contno,t1.c_conidcard18len,t1.c_billsendflag,t1.c_callcenter,t1.c_internet,t1.c_billsendpass,t1.c_nationality,t1.c_lawname,t1.c_lawidtype,t1.c_lawidno,t1.c_broker,t1.c_recommender,t1.c_recommendertype,t1.c_cityno,t1.c_modifyinfo,t1.c_freezecause,t1.c_specialcode,t1.c_backreason,t1.c_bourseflag,t1.c_sysrole,t1.c_instrepridcode,t1.c_instrepridtype,t1.c_hometelno,t1.c_shacco,t1.c_szacco,t1.c_minorflag,t1.c_transacttype,t1.d_contidnovaliddate,t1.d_lawidnovaliddate,t1.c_risklevel,t1.c_managerange,t1.c_controlholder,t1.c_actualcontroller,t1.c_marital,t1.l_familynum,t1.f_penates,t1.c_mediahobby,t1.c_institutiontype,t1.c_englishfirstname,t1.c_englishfamliyname,t1.c_industry,t1.c_companychar,t1.f_employeenum,t1.c_interesttype,t1.c_province,t1.c_city,t1.c_county
from ecifdb.o_s002_stas_tcustinfo t1
where t1.c_custtype = '1' --个人客户
union all
select case
           when t1.c_custtype <> '2' then t1.c_custname
           when t3.entname is not null then t3.entname
           when t2.entname is not null then t2.entname
           else t1.c_custname end     as name
     , t1.oc_date
     , t1.c_custno
     , t1.c_fundacco
     , t1.c_accounttype
     , t1.c_custtype
     , t1.c_custname
     , t1.c_shortname
     , case
           when t3.entname is not null then 'B'
           else t1.c_identitytype end as c_identitytype
     , t1.c_identityno
     , t1.d_idnovaliddate
     , t1.c_idcard18len
     , t1.d_opendate
     , t1.d_lastmodify
     , t1.c_accostatus
     , t1.l_changetime
     , t1.c_zipcode
     , t1.c_address
     , t1.c_phone
     , t1.c_faxno
     , t1.c_mobileno
     , t1.c_email
     , t1.c_sex
     , t1.c_birthday
     , t1.c_vocation
     , t1.c_education
     , t1.c_income
     , t1.c_corpname
     , t1.c_corptel
     , t1.c_contact
     , t1.c_contype
     , t1.c_contno
     , t1.c_conidcard18len
     , t1.c_billsendflag
     , t1.c_callcenter
     , t1.c_internet
     , t1.c_billsendpass
     , t1.c_nationality
     , t1.c_lawname
     , t1.c_lawidtype
     , t1.c_lawidno
     , t1.c_broker
     , t1.c_recommender
     , t1.c_recommendertype
     , t1.c_cityno
     , t1.c_modifyinfo
     , t1.c_freezecause
     , t1.c_specialcode
     , t1.c_backreason
     , t1.c_bourseflag
     , t1.c_sysrole
     , t1.c_instrepridcode
     , t1.c_instrepridtype
     , t1.c_hometelno
     , t1.c_shacco
     , t1.c_szacco
     , t1.c_minorflag
     , t1.c_transacttype
     , t1.d_contidnovaliddate
     , t1.d_lawidnovaliddate
     , t1.c_risklevel
     , t1.c_managerange
     , t1.c_controlholder
     , t1.c_actualcontroller
     , t1.c_marital
     , t1.l_familynum
     , t1.f_penates
     , t1.c_mediahobby
     , t1.c_institutiontype
     , t1.c_englishfirstname
     , t1.c_englishfamliyname
     , t1.c_industry
     , t1.c_companychar
     , t1.f_employeenum
     , t1.c_interesttype
     , t1.c_province
     , t1.c_city
     , t1.c_county
from ecifdb.o_s002_stas_tcustinfo t1
         left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.c_identityno = t2.regno and t2.regno <> '' -- 营业执照
         left join ecifdb.o_s099_zj_xyzq_ent_info t3
                   on t1.c_identityno = t3.credit_code and t3.credit_code <> '' --统一社会信用代码
where t1.c_custtype <> '1'
  and t1.c_identitytype = '1' --营业执照
union all
select nvl(t2.entname, t1.c_custname) as name, t1.oc_date,t1.c_custno,t1.c_fundacco,t1.c_accounttype,t1.c_custtype,t1.c_custname,t1.c_shortname,t1.c_identitytype,t1.c_identityno,t1.d_idnovaliddate,t1.c_idcard18len,t1.d_opendate,t1.d_lastmodify,t1.c_accostatus,t1.l_changetime,t1.c_zipcode,t1.c_address,t1.c_phone,t1.c_faxno,t1.c_mobileno,t1.c_email,t1.c_sex,t1.c_birthday,t1.c_vocation,t1.c_education,t1.c_income,t1.c_corpname,t1.c_corptel,t1.c_contact,t1.c_contype,t1.c_contno,t1.c_conidcard18len,t1.c_billsendflag,t1.c_callcenter,t1.c_internet,t1.c_billsendpass,t1.c_nationality,t1.c_lawname,t1.c_lawidtype,t1.c_lawidno,t1.c_broker,t1.c_recommender,t1.c_recommendertype,t1.c_cityno,t1.c_modifyinfo,t1.c_freezecause,t1.c_specialcode,t1.c_backreason,t1.c_bourseflag,t1.c_sysrole,t1.c_instrepridcode,t1.c_instrepridtype,t1.c_hometelno,t1.c_shacco,t1.c_szacco,t1.c_minorflag,t1.c_transacttype,t1.d_contidnovaliddate,t1.d_lawidnovaliddate,t1.c_risklevel,t1.c_managerange,t1.c_controlholder,t1.c_actualcontroller,t1.c_marital,t1.l_familynum,t1.f_penates,t1.c_mediahobby,t1.c_institutiontype,t1.c_englishfirstname,t1.c_englishfamliyname,t1.c_industry,t1.c_companychar,t1.f_employeenum,t1.c_interesttype,t1.c_province,t1.c_city,t1.c_county
from ecifdb.o_s002_stas_tcustinfo t1
         left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.c_identityno = t2.org_inst_cd and t2.org_inst_cd <> ''
where t1.c_custtype <> '1'
  and t1.c_identitytype = '0' -- 组织机构代码
union all
select t1.c_custname as name, t1.oc_date,t1.c_custno,t1.c_fundacco,t1.c_accounttype,t1.c_custtype,t1.c_custname,t1.c_shortname,t1.c_identitytype,t1.c_identityno,t1.d_idnovaliddate,t1.c_idcard18len,t1.d_opendate,t1.d_lastmodify,t1.c_accostatus,t1.l_changetime,t1.c_zipcode,t1.c_address,t1.c_phone,t1.c_faxno,t1.c_mobileno,t1.c_email,t1.c_sex,t1.c_birthday,t1.c_vocation,t1.c_education,t1.c_income,t1.c_corpname,t1.c_corptel,t1.c_contact,t1.c_contype,t1.c_contno,t1.c_conidcard18len,t1.c_billsendflag,t1.c_callcenter,t1.c_internet,t1.c_billsendpass,t1.c_nationality,t1.c_lawname,t1.c_lawidtype,t1.c_lawidno,t1.c_broker,t1.c_recommender,t1.c_recommendertype,t1.c_cityno,t1.c_modifyinfo,t1.c_freezecause,t1.c_specialcode,t1.c_backreason,t1.c_bourseflag,t1.c_sysrole,t1.c_instrepridcode,t1.c_instrepridtype,t1.c_hometelno,t1.c_shacco,t1.c_szacco,t1.c_minorflag,t1.c_transacttype,t1.d_contidnovaliddate,t1.d_lawidnovaliddate,t1.c_risklevel,t1.c_managerange,t1.c_controlholder,t1.c_actualcontroller,t1.c_marital,t1.l_familynum,t1.f_penates,t1.c_mediahobby,t1.c_institutiontype,t1.c_englishfirstname,t1.c_englishfamliyname,t1.c_industry,t1.c_companychar,t1.f_employeenum,t1.c_interesttype,t1.c_province,t1.c_city,t1.c_county
from ecifdb.o_s002_stas_tcustinfo t1
where t1.c_custtype <> '1'
  and t1.c_identitytype not in ('1', '2');
--其它
-------- S002 ---------


-------- S006 ---------
select "开始执行s006 证件类型清洗";
drop table if exists ecifdb.t_s006_qh_bs_customer;
create table ecifdb.t_s006_qh_bs_customer as
select t1.investorname as name, null as licenseno_type, t1.oc_date,t1.tradingday,t1.investorname,t1.identifiedcardno,t1.investorid,t1.city,t1.address,t1.zipcode,t1.telephone,t1.cancelflag,t1.issettlement,t1.investortype,t1.activedate,t1.eid,t1.licenseno,t1.openinvestorname,t1.openidentifiedcardno,t1.freezestatus,t1.clientregion,t1.national,t1.passport,t1.businessregistration,t1.isseconderyagent,t1.csrcsecagentid,t1.isassetmgrins,t1.stkaccountid,t1.stkopendate,t1.ismarketmaker,t1.overseasinstitutiontype
from ecifdb.o_s006_qh_bs_customer t1
where t1.investortype = '0' -- 个人
union all
select case
           when t1.investortype not in ('2', '3', '4', '5', '6', '9') then t1.investorname
           when t3.entname is not null then t3.entname --统一社会信用代码
           when t4.entname is not null then t4.entname --组织机构代码
           when t2.entname is not null then t2.entname --营业执照
           else t1.investorname end as name
     , case
           when t3.entname is not null then '2810'
           else '2010' end          as licenseno_type
     , t1.oc_date
     , t1.tradingday
     , t1.investorname
     , t1.identifiedcardno
     , t1.investorid
     , t1.city
     , t1.address
     , t1.zipcode
     , t1.telephone
     , t1.cancelflag
     , t1.issettlement
     , t1.investortype
     , t1.activedate
     , t1.eid
     , t1.licenseno
     , t1.openinvestorname
     , t1.openidentifiedcardno
     , t1.freezestatus
     , t1.clientregion
     , t1.national
     , t1.passport
     , t1.businessregistration
     , t1.isseconderyagent
     , t1.csrcsecagentid
     , t1.isassetmgrins
     , t1.stkaccountid
     , t1.stkopendate
     , t1.ismarketmaker
     , t1.overseasinstitutiontype
from ecifdb.o_s006_qh_bs_customer t1
         left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.licenseno = t2.regno and t2.regno <> '' --营业执照
         left join ecifdb.o_s099_zj_xyzq_ent_info t3 on t1.licenseno = t3.credit_code and t3.credit_code <> '' --统一社会信用代码
         left join ecifdb.o_s099_zj_xyzq_ent_info t4 on t1.eid = t4.org_inst_cd and t4.org_inst_cd <> '' -- 组织机构代码
where t1.investortype <> '0' ; -- 除个人外的所有产品和企业机构
-------- S006 ---------


-------- S008 ---------
select "开始执行s008 证件类型清洗";
drop table if exists ecifdb.t_s008_bib_t_cm_clientinfo;
create table ecifdb.t_s008_bib_t_cm_clientinfo as
select t1.oc_date
     , t1.client_id
     , t1.client_name
     , t1.respon_org
     , t1.create_employee
     , t1.create_time
     , t1.update_employee
     , t1.update_time
     , t1.client_simname
     , t1.client_simdesc
     , t1.client_area
     , t1.client_addr
     , t1.client_zipcode
     , t1.client_tel
     , t1.client_fax
     , t1.client_majorcate
     , t1.client_subcate
     , t1.client_seriescate
     , t1.hobby_desc
     , t1.rela_client
     , t1.business_progress
     , t1.require_desc
     , t1.remark
     , t1.rela_information
     , t1.parent_id
     , t1.fund_account
     , t1.rec_status
     , t1.client_level
     , t1.sys
     , t1.open_date
     , t1.proxy_name
     , t1.proxy_idkind
     , t1.proxy_idno
     , t1.proxy_idvalid
     , t1.client_simname_web
     , t1.client_addr_web
     , t1.client_zipcode_web
     , t1.client_tel_web
     , t1.client_fax_web
     , t1.client_phone
     , t1.client_phone_web
     , t1.client_email
     , t1.client_email_web
     , t1.id_valid
     , t1.ta_account
     , t1.is_specfinance
     , t1.sex
     , t1.birthday
     , t1.client_profession
     , t1.client_education
     , t1.stockaccount_sh
     , t1.stockaccount_sz
     , t1.first_invest_date
     , t1.work_name
     , t1.work_tel
     , t1.account_type
     , t1.prod_switch
     , case
           when t2.entname is not null then '2810'
           else t1.id_kind end as id_kind
     , t1.id_no
     , t1.client_status
     , t1.clientid_gt
     , t1.vendor
     , t1.branch_name
     , t1.issend_statement
     , t1.city_no
     , t1.client_source_org
     , t1.commitment_business
     , t1.business_opportunity
     , t1.sw_industry_code
     , t1.out_respon_business
     , t1.source_flag
     , t1.total_asset
     , t1.reg_capital
     , t1.recent_annual_income
     , t1.recent_aftertax_profit
     , t1.client_majorcate_bak
     , t1.client_subcate_bak
     , t1.client_seriescate_bak
     , t1.stock_code
     , t1.belong_salesdepart
     , t1.update_flag
     , t1.district_no
     , t1.ta4_fundaccount
     , t1.qfii_custid
     , t1.tas_agency
     , t1.tas_net
     , t1.ta4_agency
     , t1.ta4_net
     , t1.is_public_company
     , t1.client_label
     , t1.sub_rating
     , t1.userdefined_label
     , t1.client_suitability
     , t1.aml_risk_level
from ecifdb.o_s008_bib_t_cm_clientinfo t1
         left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.id_no = t2.credit_code and t2.credit_code <> ''
where t1.id_kind in ('26', '8') -- 7 是组织机构代码， 8 是 营业执照， 里面都包含统一社会信用代码。
union all
select t1.oc_date,t1.client_id,t1.client_name,t1.respon_org,t1.create_employee,t1.create_time,t1.update_employee,t1.update_time,t1.client_simname,t1.client_simdesc,t1.client_area,t1.client_addr,t1.client_zipcode,t1.client_tel,t1.client_fax,t1.client_majorcate,t1.client_subcate,t1.client_seriescate,t1.hobby_desc,t1.rela_client,t1.business_progress,t1.require_desc,t1.remark,t1.rela_information,t1.parent_id,t1.fund_account,t1.rec_status,t1.client_level,t1.sys,t1.open_date,t1.proxy_name,t1.proxy_idkind,t1.proxy_idno,t1.proxy_idvalid,t1.client_simname_web,t1.client_addr_web,t1.client_zipcode_web,t1.client_tel_web,t1.client_fax_web,t1.client_phone,t1.client_phone_web,t1.client_email,t1.client_email_web,t1.id_valid,t1.ta_account,t1.is_specfinance,t1.sex,t1.birthday,t1.client_profession,t1.client_education,t1.stockaccount_sh,t1.stockaccount_sz,t1.first_invest_date,t1.work_name,t1.work_tel,t1.account_type,t1.prod_switch,t1.id_kind,t1.id_no,t1.client_status,t1.clientid_gt,t1.vendor,t1.branch_name,t1.issend_statement,t1.city_no,t1.client_source_org,t1.commitment_business,t1.business_opportunity,t1.sw_industry_code,t1.out_respon_business,t1.source_flag,t1.total_asset,t1.reg_capital,t1.recent_annual_income,t1.recent_aftertax_profit,t1.client_majorcate_bak,t1.client_subcate_bak,t1.client_seriescate_bak,t1.stock_code,t1.belong_salesdepart,t1.update_flag,t1.district_no,t1.ta4_fundaccount,t1.qfii_custid,t1.tas_agency,t1.tas_net,t1.ta4_agency,t1.ta4_net,t1.is_public_company,t1.client_label,t1.sub_rating,t1.userdefined_label,t1.client_suitability,t1.aml_risk_level
from ecifdb.o_s008_bib_t_cm_clientinfo where id_kind not in ('26', '8')
;

drop table if exists ecifdb.t_s008_bib_t_cm_clientcompaddi;
create table ecifdb.t_s008_bib_t_cm_clientcompaddi as
select t1.oc_date
    , t1.client_id
    , t1.is_listed_comp
    , t1.actual_controller
    , t1.actctrl_sharehd_ratio
    , t1.is_govholding
    , t1.localgov_adminlevel
    , t1.budget_revenue_year
    , t1.budget_revenue_amount
    , t1.netassets_month
    , t1.netassets_amount
    , t1.asset_liability_month
    , t1.asset_liability_ratio
    , t1.mainbusirev_year
    , t1.mainbusirev_amount
    , t1.netprofit_year
    , t1.netprofit_amount
    , t1.oper_cashflow_year
    , t1.oper_cashflow_amount
    , t1.localgov_gdp
    , t1.esti_debt_per
    , t1.is_debt_outstand
    , t1.has_major_violations
    , t1.has_dishonesty
    , t1.has_spec_situation
    , t1.appro_invest_req
    , t1.fund_source
    , t1.unified_socialcredit_code
    , t1.client_nationality
    , t1.is_threeinonecode
    , t1.business_licence
    , t1.organization_code
    , t1.tax_registration_certificate
    , t1.registered_addr
    , t1.registered_zipcode
    , t1.is_militaryenterprise
    , t1.is_secretenterprise
    , t1.id_begin_date
    , t1.id_end_date
    , t1.attachment_ids
    , t1.is_id_valid_permanet
    , t1.regno
    , case when t2.entname is not null then '2810' else '2010' end as business_licence_type
    , case when t3.entname is not null then '2810' else '2020' end as organization_code_type
from ecifdb.o_s008_bib_t_cm_clientcompaddi t1
    left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.business_licence = t2.credit_code and t2.credit_code <> ''
    left join ecifdb.o_s099_zj_xyzq_ent_info t3 on t1.organization_code = t3.credit_code and t3.credit_code <> '';

drop table if exists ecifdb.t_s008_bib_t_client_suitability_info;
create table ecifdb.t_s008_bib_t_client_suitability_info as
select t1.oc_date
    , t1.client_id
    , t1.registered_addr
    , t1.client_type
    , t1.business_scope
    , t1.income_source
    , t1.income_amount
    , t1.asset_amount
    , t1.asset_comment
    , t1.debt_amount
    , t1.net_asset_amount
    , t1.avg_annual_income
    , t1.income_for_invest_rate
    , t1.invest_horizon
    , t1.invest_product_type
    , t1.invest_product_amount
    , t1.invest_market
    , t1.invest_diver
    , t1.expected_benefit_rate
    , t1.attachment_ids
    , t1.actual_controller
    , t1.actual_beneficiary
    , t1.credit_record
    , t1.investor_access_info
    , t1.is_all_factor_covered
    , t1.update_by
    , t1.update_date
    , t1.client_age
    , t1.actual_controller_id_kind
    , t1.actual_controller_id_no
    , t1.actual_controller_id_valid
    , t1.client_nationality
    , t1.organization_code
    , t1.tax_registration_certificate
    , t1.actual_controller_email
    , t1.actual_controller_phone
    , t1.actual_controller_addr
    , t1.actual_controller_person
    , t1.business_licence
    , t1.is_required_factor_covered
    , case when t2.entname is not null then '2810' else '2010' end as business_licence_type
    , case when t3.entname is not null then '2810' else '2020' end as organization_code_type
from ecifdb.o_s008_bib_t_client_suitability_info t1
     left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.business_licence = t2.credit_code and t2.credit_code <> ''
     left join ecifdb.o_s099_zj_xyzq_ent_info t3 on t1.organization_code = t3.credit_code and t3.credit_code <> '';

-------- S008 ---------


-------- S009 ---------
select "开始执行s009 证件类型清洗";
drop table if exists ecifdb.t_s009_oms_organinfo;
create table ecifdb.t_s009_oms_organinfo as
select t1.oc_date                     --业务日期
     , t1.client_id                   --客户编号
     , t1.branch_no                   --营业部号
     , t1.organ_code                  --机构代码
     , t1.organ_audit_date            --机构年审日期
     , t1.organ_code_begin_date       --机构代码有效起始日
     , t1.organ_code_end_date         --机构代码有效截止日
     , t1.tax_register                --国税税务登记号码
     , t1.tax_registe_begin_date      --国税登记开始日期
     , t1.tax_registe_end_date        --国税登记结束日期
     , t1.work_status_name            --经营状态说明
     , t1.business_licence            --营业执照
     , case
           when t2.oc_date is not null then '3'
           when t3.oc_date is not null then 'P'
           else '2'
    end as business_licence_kind      --证件类型
     , t1.business_licence_begin_date --营业执照开始日期
     , t1.business_licence_end_date   --营业执照截止日期
     , t1.company_kind                --企业性质
     , t1.work_range                  --经营范围
     , t1.register_fund               --注册资本
     , t1.register_money_type         --注册资本币种
     , t1.home_page                   --网站地址
     , t1.id_address                  --身份证地址
     , t1.address                     --联系地址
     , t1.zipcode                     --邮政编码
     , t1.fax                         --传真号码
     , t1.e_mail                      --电子信箱
     , t1.statement_flag              --对账单寄送选择
     , t1.account_data                --开户规范信息
     , t1.risk_info                   --风险要素信息
     , t1.msn_id                      --MSN账号
     , t1.skype_id                    --Skype账号
     , t1.developer                   --开发人员
     , t1.develop_source              --客户来源
     , t1.cost_place                  --成本中心
     , t1.inst_name                   --法人代表姓名
     , t1.inst_id_kind                --法人代表证件类型
     , t1.inst_id_no                  --法人代表证件号码
     , t1.inst_id_begin_date          --法人代表证件有效起始日
     , t1.inst_id_end_date            --法人代表证件有效截至日
     , t1.inst_tel                    --法人代表电话号码
     , t1.contact_name                --经办人姓名
     , t1.contact_id_kind             --经办人证件类型
     , t1.contact_id_no               --经办人证件号码
     , t1.contact_id_begin_date       --经办人证件有效起始日
     , t1.contact_id_end_date         --经办人证件有效截至日
     , t1.contact_tel                 --经办人电话号码
     , t1.relation_name               --联系人姓名
     , t1.relation_id_kind            --联系人证件类型
     , t1.relation_id_no              --联系人证件号码
     , t1.relation_id_begin_date      --联系人证件有效起始日期
     , t1.relation_id_end_date        --联系人证件有效截至日期
     , t1.relation_tel                --联系人电话号码
     , t1.control_holder              --控股股东
     , t1.control_id_kind             --控股股东证件类型
     , t1.control_id_no               --控股股东证件号码
     , t1.control_id_begin_date       --控股股东证件开始日
     , t1.control_id_end_date         --控股股东证件结束日
     , t1.duty_name                   --负责人姓名
     , t1.duty_id_kind                --负责人证件类型
     , t1.duty_id_no                  --负责人证件号码
     , t1.duty_id_begin_date          --负责人证件开始日
     , t1.duty_id_end_date            --负责人证件结束日
     , t1.duty_tel                    --负责人联系电话
     , t1.prove_id_kind               --证明文件类型
     , t1.prove_id_no                 --证明文件号码
     , t1.prove_id_begin_date         --证明文件有效开始日期
     , t1.prove_id_end_date           --证明文件有效结束日期
     , t1.remark                      --备注说明
     , t1.position_str                --定位串
     , t1.city_no                     --城市编号
     , t1.industry_type               --行业类别
     , t1.market_code                 --上市股票代码
     , t1.regtax_register             --地税税务登记号
     , t1.regtax_registe_begin_date   --地税登记开始日期
     , t1.regtax_registe_end_date     --地税登记结束日期
     , t1.business_audit_date         --营业执照年审日期
     , t1.industry_range              --行业信息
     , t1.issue_depart                --签发机关
     , t1.contact_freq                --联络频率
     , t1.state_owned_prop            --国有属性
     , t1.market_prop                 --上市属性
     , t1.register_fund_prop          --注册资金属性
     , t1.benefit_person              --受益人
     , t1.mobile_tel                  --手机号码
     , t1.office_tel                  --单位电话
     , t1.market_place                --上市地点
     , t1.en_contact_type             --允许联络方式
from ecifdb.o_s009_oms_organinfo t1
         left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.business_licence = t2.credit_code and t2.credit_code <> ''
         left join ecifdb.o_s099_zj_xyzq_ent_info t3 on t1.business_licence = t3.org_inst_cd and t3.org_inst_cd <> '';


drop table if exists ecifdb.o_s009_cust_info_type;
create table ecifdb.o_s009_cust_info_type as
select t1.name
     , t1.oc_date
     , t1.client_id
     , t1.branch_no
     , t1.dev_branch_no
     , t1.client_card
     , t1.client_name
     , t1.full_name
     , t1.corp_client_group
     , t1.corp_risk_level
     , t1.asset_level
     , t1.client_gender
     , t1.nationality
     , t1.organ_flag
     , case
           when t2.oc_date is not null then '3'
           when t3.oc_date is not null then 'P'
           else t1.id_kind end as id_kind
     , t1.id_no
     , t1.id_begin_date
     , t1.id_end_date
     , t1.open_date
     , t1.cancel_date
     , t1.confirm_date
     , t1.client_status
     , t1.position_str
     , t1.aml_risk_level
     , t1.corp_begin_date
     , t1.corp_end_date
     , t1.lic
     , t1.org_cd
from ecifdb.o_s009_cust_info t1
         left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.id_no = t2.credit_code and t2.credit_code <> ''
         left join ecifdb.o_s099_zj_xyzq_ent_info t3 on t1.id_no = t3.org_inst_cd and t3.org_inst_cd <> ''
where t1.id_kind = '2'
union all
select t1.name,t1.oc_date,t1.client_id,t1.branch_no,t1.dev_branch_no,t1.client_card,t1.client_name,t1.full_name,t1.corp_client_group,t1.corp_risk_level,t1.asset_level,t1.client_gender,t1.nationality,t1.organ_flag,t1.id_kind,t1.id_no,t1.id_begin_date,t1.id_end_date,t1.open_date,t1.cancel_date,t1.confirm_date,t1.client_status,t1.position_str,t1.aml_risk_level,t1.corp_begin_date,t1.corp_end_date,t1.lic,t1.org_cd
from ecifdb.o_s009_cust_info
where id_kind <> '2';

-------- S009 ---------


-------- S010 ---------
select "开始执行s010 证件类型清洗";
drop table if exists ecifdb.t_s010_hjs_customer_info;
create table ecifdb.t_s010_hjs_customer_info as
select t1.oc_date
     , t1.khh
     , t1.jgbz
     , t1.khxm
     , t1.khqc
     , case when t2.entname is not null then '19' else t1.zjlb end as zjlb
     , t1.zjbh
     , t1.zjdz
     , t1.dh
     , t1.cz
     , t1.province
     , t1.city
     , t1.sec
     , t1.yzbm
     , t1.dz
     , t1.khrq
     , t1.xhrq
     , t1.khzt
     , t1.yyb
     , t1.fxjb
     , t1.xldm
     , t1.zydm
     , t1.gjdm
     , t1.email
     , t1.mobile
     , t1.xb
     , t1.frdb
     , t1.zjlb_frdb
     , t1.zjbh_frdb
     , t1.hylb
     , t1.jyfw
     , t1.zcsj
     , t1.zczb
     , t1.jbrxm
     , t1.zjlb_jbr
     , t1.zjbh_jbr
     , t1.gqkhh
     , t1.jjkhh
     , t1.tpkhh
     , t1.gmt_modify
from ecifdb.o_s010_hjs_customer_info t1
         left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.zjbh = t2.credit_code and t2.credit_code <> ''
where t1.zjlb = '8' --营业执照里面包含统一社会信用代码
union all
select t1.oc_date
     , t1.khh
     , t1.jgbz
     , t1.khxm
     , t1.khqc
     , case when t2.entname is not null then '7' else t1.zjlb end as zjlb
     , t1.zjbh
     , t1.zjdz
     , t1.dh
     , t1.cz
     , t1.province
     , t1.city
     , t1.sec
     , t1.yzbm
     , t1.dz
     , t1.khrq
     , t1.xhrq
     , t1.khzt
     , t1.yyb
     , t1.fxjb
     , t1.xldm
     , t1.zydm
     , t1.gjdm
     , t1.email
     , t1.mobile
     , t1.xb
     , t1.frdb
     , t1.zjlb_frdb
     , t1.zjbh_frdb
     , t1.hylb
     , t1.jyfw
     , t1.zcsj
     , t1.zczb
     , t1.jbrxm
     , t1.zjlb_jbr
     , t1.zjbh_jbr
     , t1.gqkhh
     , t1.jjkhh
     , t1.tpkhh
     , t1.gmt_modify
from ecifdb.o_s010_hjs_customer_info t1
         left join ecifdb.o_s099_zj_xyzq_ent_info t2 on t1.zjbh = t2.org_inst_cd and t2.org_inst_cd <> ''
where t1.zjlb = '19' --统一社会信用代码里面包含组织机构代码
union all
select t1.oc_date,t1.khh,t1.jgbz,t1.khxm,t1.khqc,t1.zjlb,t1.zjbh,t1.zjdz,t1.dh,t1.cz,t1.province,t1.city,t1.sec,t1.yzbm,t1.dz,t1.khrq,t1.xhrq,t1.khzt,t1.yyb,t1.fxjb,t1.xldm,t1.zydm,t1.gjdm,t1.email,t1.mobile,t1.xb,t1.frdb,t1.zjlb_frdb,t1.zjbh_frdb,t1.hylb,t1.jyfw,t1.zcsj,t1.zczb,t1.jbrxm,t1.zjlb_jbr,t1.zjbh_jbr,t1.gqkhh,t1.jjkhh,t1.tpkhh,t1.gmt_modify
from ecifdb.o_s010_hjs_customer_info t1
where t1.zjlb not in ('8', '19');

-------- S010 ---------