/*
Navicat MySQL Data Transfer

Source Server         : 192.25.106.53
Source Server Version : 50633
Source Host           : 192.25.106.53:3306
Source Database       : ecifdb

Target Server Type    : MYSQL
Target Server Version : 50633
File Encoding         : 65001

Date: 2018-12-20 09:31:46
*/



SET FOREIGN_KEY_CHECKS=0;
USE ecifdb;

-- ----------------------------
-- Table structure for merge_cust_map
-- ----------------------------
DROP TABLE IF EXISTS `merge_cust_map`;
CREATE TABLE `merge_cust_map` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` varchar(32) NOT NULL,
  `cust_id` varchar(32) NOT NULL,
  `field_name` varchar(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2708 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for merge_cust_relation
-- ----------------------------
DROP TABLE IF EXISTS `merge_cust_relation`;
CREATE TABLE `merge_cust_relation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` varchar(32) NOT NULL,
  `cust_id` varchar(32) NOT NULL,
  `group_main` varchar(2) NOT NULL,
  `split_status` varchar(2) NOT NULL,
  `create_user` varchar(20) DEFAULT NULL,
  `create_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=575 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_col_field_priority
-- ----------------------------
DROP TABLE IF EXISTS `t_col_field_priority`;
CREATE TABLE `t_col_field_priority` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `type_id` varchar(100) NOT NULL COMMENT '类型',
  `tab_name` varchar(100) DEFAULT NULL COMMENT '名称',
  `code_id` varchar(11) DEFAULT NULL COMMENT '代码',
  `field_priority` int(11) DEFAULT NULL COMMENT '字段优先级别',
  `status` varchar(2) DEFAULT NULL COMMENT '有效标志',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `create_user` varchar(100) DEFAULT NULL COMMENT '创建人',
  `update_user` varchar(100) DEFAULT NULL COMMENT '更细人',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=119 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='字段优先级表';

-- ----------------------------
-- Table structure for t_sys_account_info
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_account_info`;
CREATE TABLE `t_sys_account_info` (
  `CUR_ID` varchar(32) NOT NULL,
  `ACCOUNTID` varchar(50) NOT NULL,
  `CHANNEL` varchar(32) DEFAULT NULL,
  `PASSWORD` varchar(50) DEFAULT NULL,
  `COMPANYID` varchar(32) DEFAULT NULL,
  `ACCOUNT` varchar(50) DEFAULT NULL,
  `STATUS` varchar(11) DEFAULT NULL,
  `REALNAME` varchar(20) DEFAULT NULL,
  `SEXUALITY` varchar(11) DEFAULT NULL,
  `STAFFID` varchar(32) DEFAULT NULL,
  `PASSWORD1` varchar(100) DEFAULT NULL,
  `SYSTYPE` char(2) DEFAULT NULL,
  `REMARK` varchar(100) DEFAULT NULL,
  `INFO` varchar(100) DEFAULT NULL,
  `CREATEDTIME` char(19) DEFAULT NULL,
  PRIMARY KEY (`CUR_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_sys_authorityfor_role
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_authorityfor_role`;
CREATE TABLE `t_sys_authorityfor_role` (
  `CUR_ID` varchar(32) NOT NULL DEFAULT '' COMMENT '主键',
  `ROLEID` varchar(50) NOT NULL COMMENT '角色标识',
  `SUBSITEID` varchar(32) DEFAULT NULL COMMENT '菜单标识',
  `MENUID` varchar(32) DEFAULT NULL COMMENT '菜单信息',
  `PAGEID` varchar(32) DEFAULT NULL COMMENT '页面标识',
  `FUNCTIONALID` varchar(32) DEFAULT NULL COMMENT '按钮标识',
  PRIMARY KEY (`CUR_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_sys_code_detail
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_code_detail`;
CREATE TABLE `t_sys_code_detail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `type_id` varchar(20) COLLATE utf8 DEFAULT NULL COMMENT '代码分类编号',
  `code_id` varchar(50) COLLATE utf8 DEFAULT NULL COMMENT '代码值编码',
  `code_name` varchar(100) COLLATE utf8 DEFAULT NULL COMMENT '代码值名称',
  `parent_id` varchar(20) COLLATE utf8 DEFAULT NULL COMMENT '父级编号',
  `status` varchar(2) COLLATE utf8 DEFAULT NULL COMMENT '有效标志',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `create_user` varchar(100) COLLATE utf8 DEFAULT NULL COMMENT '创建人',
  `update_user` varchar(100) COLLATE utf8 DEFAULT NULL COMMENT '更新人',
  `ylzd1` varchar(200) COLLATE utf8 DEFAULT NULL,
  `ylzd2` varchar(200) COLLATE utf8 DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_t_sys_code_detail_01` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7078 DEFAULT CHARSET=utf8 COLLATE=utf8 COMMENT='码值明细表';

-- ----------------------------
-- Table structure for t_sys_code_sourcerelation
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_code_sourcerelation`;
CREATE TABLE `t_sys_code_sourcerelation` (
  `id` int(8) NOT NULL AUTO_INCREMENT,
  `type_id` varchar(20) COLLATE utf8 DEFAULT NULL COMMENT '代码分类编号',
  `code_id` varchar(50) COLLATE utf8 DEFAULT NULL COMMENT '代码值编码',
  `code_name` varchar(100) COLLATE utf8 DEFAULT NULL COMMENT '代码值名称',
  `source_sys` varchar(20) COLLATE utf8 DEFAULT NULL COMMENT '来源系统',
  `source_code_id` varchar(50) COLLATE utf8 DEFAULT NULL COMMENT '来源系统代码值编码',
  `source_code_name` varchar(100) COLLATE utf8 DEFAULT NULL COMMENT '来源系统代码值名称',
  `status` varchar(2) COLLATE utf8 DEFAULT NULL COMMENT '有效标志',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `create_user` varchar(100) COLLATE utf8 DEFAULT NULL COMMENT '创建人',
  `update_user` varchar(100) COLLATE utf8 DEFAULT NULL COMMENT '更新人',
  `ylzd1` varchar(200) COLLATE utf8 DEFAULT NULL,
  `ylzd2` varchar(200) COLLATE utf8 DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_t_sys_code_sourcerelation_1` (`type_id`,`code_id`,`source_sys`,`source_code_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5067 DEFAULT CHARSET=utf8 COLLATE=utf8 COMMENT='码值映射表';


-- ----------------------------
-- Table structure for t_sys_code_type
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_code_type`;
CREATE TABLE `t_sys_code_type` (
  `id` int(8) NOT NULL AUTO_INCREMENT,
  `type_id` varchar(20) COLLATE utf8 DEFAULT NULL COMMENT '代码分类编号',
  `type_name` varchar(200) COLLATE utf8 DEFAULT NULL COMMENT '代码分类名称',
  `status` varchar(2) COLLATE utf8 DEFAULT NULL COMMENT '有效标志',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `create_user` varchar(100) COLLATE utf8 DEFAULT NULL COMMENT '创建人',
  `update_user` varchar(100) COLLATE utf8 DEFAULT NULL COMMENT '更新人',
  `ylzd1` varchar(200) COLLATE utf8 DEFAULT NULL,
  `ylzd2` varchar(200) COLLATE utf8 DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=155 DEFAULT CHARSET=utf8 COLLATE=utf8 COMMENT='码值分类表';


-- ----------------------------
-- Table structure for t_sys_group_permission
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_group_permission`;
CREATE TABLE `t_sys_group_permission` (
  `id` int(8) NOT NULL AUTO_INCREMENT,
  `group_id` varchar(50) DEFAULT NULL COMMENT '群组编号',
  `access_group_id` varchar(50) DEFAULT NULL COMMENT '允许查看群组编号',
  `super_group` varchar(1) DEFAULT NULL,
  `status` varchar(1) DEFAULT NULL COMMENT '有效标志',
  `create_user` varchar(255) DEFAULT NULL COMMENT '创建人',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_user` varchar(255) DEFAULT NULL COMMENT '更新人',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=29524 DEFAULT CHARSET=utf8 COMMENT='群组权限配置表';


-- ----------------------------
-- Table structure for t_sys_log
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_log`;
CREATE TABLE `t_sys_log` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `USERID` varchar(32) NOT NULL,
  `CUSTOMERTYPE` varchar(32) NOT NULL,
  `CUSTTYPE` varchar(100) DEFAULT NULL,
  `CUST_ID` varchar(100) DEFAULT NULL,
  `CUSTNAME` varchar(200) DEFAULT NULL,
  `IDNUMBER` varchar(200) DEFAULT NULL,
  `CREATETIME` datetime NOT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=2406 DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for t_sys_org
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_org`;
CREATE TABLE `t_sys_org` (
  `org_id` varchar(50) NOT NULL COMMENT '组织号',
  `org_name` varchar(255) DEFAULT NULL COMMENT '组织名称',
  `branch_no` varchar(50) DEFAULT NULL COMMENT '营业部号',
  `status` varchar(1) DEFAULT NULL COMMENT '状态',
  `parent_org_id` varchar(50) DEFAULT NULL COMMENT '上级组织号',
  `parent_org_name` varchar(255) DEFAULT NULL COMMENT '上级组织名称',
  `group_id` varchar(50) DEFAULT NULL COMMENT '组号',
  `group_name` varchar(255) DEFAULT NULL COMMENT '组名称',
  `org_chain` varchar(255) DEFAULT NULL COMMENT '组织链',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`org_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='组织表';


-- ----------------------------
-- Table structure for t_sys_org_user
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_org_user`;
CREATE TABLE `t_sys_org_user` (
  `user_id` text,
  `pk_uuia_psndoc` text,
  `name` text,
  `usedname` text,
  `sex` text,
  `usercl` text,
  `permanreside` text,
  `nativeplace` text,
  `nationality` text,
  `marital` text,
  `joinworkdate` text,
  `joinsysdate` text,
  `indutydate` text,
  `health` text,
  `birthdate` text,
  `org_id` text,
  `org_name` text,
  `branch_no` text,
  `group_id` text,
  `group_name` text,
  `org_chain` text,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for t_sys_page_info
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_page_info`;
CREATE TABLE `t_sys_page_info` (
  `PAGEID` varchar(32) NOT NULL COMMENT '页面标识',
  `MENUID` varchar(32) DEFAULT NULL COMMENT '菜单标识',
  `FRONTPAGEID` varchar(32) DEFAULT NULL COMMENT '上一条页面标识',
  `PAGETITLE` varchar(100) DEFAULT NULL COMMENT '页面标题名称',
  `STEPNUMBE` int(11) NOT NULL AUTO_INCREMENT COMMENT '页面顺序',
  `DESCRIPTION` varchar(200) DEFAULT NULL COMMENT '页面描述',
  `PAGEPATH` varchar(200) DEFAULT NULL COMMENT '页面路径',
  `DELETED` varchar(8) DEFAULT NULL COMMENT '删除状态',
  `SUBSITEID` varchar(32) DEFAULT NULL COMMENT '父级菜单标识',
  PRIMARY KEY (`PAGEID`),
  UNIQUE KEY `STEPNUMBE` (`STEPNUMBE`)
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for t_sys_priority
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_priority`;
CREATE TABLE `t_sys_priority` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sys_name` varchar(100) COLLATE utf8 DEFAULT NULL COMMENT '系统名称',
  `sys_code` varchar(20) COLLATE utf8 DEFAULT NULL COMMENT '系统编码',
  `sys_priority` int(10) DEFAULT NULL COMMENT '系统优先级',
  `status` varchar(2) COLLATE utf8 DEFAULT NULL COMMENT '有效标志',
  `type` varchar(2) COLLATE utf8 DEFAULT NULL COMMENT '客户类型（01个人；02企业）',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `create_user` varchar(100) COLLATE utf8 DEFAULT NULL COMMENT '创建人',
  `update_user` varchar(100) COLLATE utf8 DEFAULT NULL COMMENT '更新人',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_t_sys_priority_01` (`sys_code`,`type`)
) ENGINE=InnoDB AUTO_INCREMENT=238 DEFAULT CHARSET=utf8 COLLATE=utf8 COMMENT='系统优先级表';


-- ----------------------------
-- Table structure for t_sys_priority_status
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_priority_status`;
CREATE TABLE `t_sys_priority_status` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `status` varchar(2) DEFAULT NULL COMMENT 'Y????? N???????',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '????',
  UNIQUE KEY `index_t_sys_priority_status_1` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for t_sys_role_account
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_role_account`;
CREATE TABLE `t_sys_role_account` (
  `ID` varchar(32) NOT NULL,
  `ACCOUNTID` varchar(50) NOT NULL,
  `ROLEID` varchar(50) NOT NULL,
  `REMARK` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for t_sys_role_info
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_role_info`;
CREATE TABLE `t_sys_role_info` (
  `ID` varchar(32) NOT NULL COMMENT '标识',
  `ROLE` varchar(50) DEFAULT '',
  `ROLEID` varchar(50) NOT NULL,
  `COMPANYID` varchar(100) DEFAULT '',
  `STATUS` varchar(8) DEFAULT NULL,
  `DESCRIPTION` varchar(100) DEFAULT '',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for t_sys_source
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_source`;
CREATE TABLE `t_sys_source` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sys_code` varchar(16) NOT NULL,
  `sys_name` varchar(32) NOT NULL,
  `status` varchar(2) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=45 DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for t_sys_subsite_info
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_subsite_info`;
CREATE TABLE `t_sys_subsite_info` (
  `SUBSITEID` varchar(32) NOT NULL COMMENT '按钮标识',
  `SUBSITECODE` varchar(32) DEFAULT NULL COMMENT '菜单编码',
  `SUBSITENAME` varchar(100) DEFAULT NULL COMMENT '菜单名称',
  `SUBSITEDESCRIPTION` varchar(200) DEFAULT NULL COMMENT '菜单描述',
  `SUBSITEPLOT` varchar(200) DEFAULT NULL COMMENT '连接端口与地址',
  `SUBSITEORDER` int(11) NOT NULL AUTO_INCREMENT COMMENT '菜单顺序',
  `COMPANYID` varchar(32) DEFAULT NULL COMMENT '项目标识码',
  `DELETED` varchar(8) DEFAULT NULL COMMENT '删除状态',
  `ICON` varchar(200) DEFAULT NULL COMMENT '显示图标路径',
  `WEBCONTEXT` varchar(100) DEFAULT NULL COMMENT 'web信息',
  `WEBPORT` varchar(32) DEFAULT NULL COMMENT 'web端口',
  `REMARK` varchar(200) DEFAULT NULL COMMENT '备注',
  PRIMARY KEY (`SUBSITEID`),
  UNIQUE KEY `SUBSITEORDER` (`SUBSITEORDER`)
) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for t_sys_user
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_user`;
CREATE TABLE `t_sys_user` (
  `user_id` varchar(10) NOT NULL COMMENT '人员编码(登陆号)',
  `pk_uuia_psndoc` varchar(50) NOT NULL COMMENT '人员主键',
  `name` varchar(100) DEFAULT NULL COMMENT '人员姓名',
  `usedname` varchar(100) DEFAULT NULL COMMENT '曾用名',
  `sex` varchar(50) DEFAULT NULL COMMENT '性别',
  `usercl` varchar(255) DEFAULT NULL COMMENT '人员类别',
  `org_id` varchar(50) NOT NULL COMMENT '组织号',
  `permanreside` varchar(255) DEFAULT NULL COMMENT '户口所在地',
  `nativeplace` varchar(255) DEFAULT NULL COMMENT '籍贯',
  `nationality` varchar(255) DEFAULT NULL COMMENT '民族',
  `marital` varchar(255) DEFAULT NULL COMMENT '婚姻状况',
  `joinworkdate` varchar(10) DEFAULT NULL COMMENT '参加工作日期',
  `joinsysdate` varchar(10) DEFAULT NULL COMMENT '进入本系统工作时间',
  `indutydate` varchar(10) DEFAULT NULL COMMENT '到职日期',
  `health` varchar(50) DEFAULT NULL COMMENT '健康状况',
  `birthdate` varchar(10) DEFAULT NULL COMMENT '出生日期',
  `status` varchar(1) DEFAULT NULL COMMENT '状态',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='人员表';


-- ----------------------------
-- Table structure for t_sys_user_login
-- ----------------------------
DROP TABLE IF EXISTS `t_sys_user_login`;
CREATE TABLE `t_sys_user_login` (
  `LOGINID` varchar(32) NOT NULL,
  `USERNAME` varchar(20) NOT NULL,
  `TOKEN` varchar(80) NOT NULL,
  `CREATETIME` datetime NOT NULL,
  PRIMARY KEY (`TOKEN`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for white_list_server
-- ----------------------------
DROP TABLE IF EXISTS `white_list_server`;
CREATE TABLE `white_list_server` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sys_code` varchar(16) NOT NULL,
  `sys_name` varchar(32) NOT NULL,
  `token` varchar(100) DEFAULT NULL,
  `ip` varchar(20) NOT NULL,
  `status` varchar(2) NOT NULL,
  `create_date` date DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=40 DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for a_ecif_etl_date
-- ----------------------------
DROP TABLE IF EXISTS `a_ecif_etl_date`;
CREATE TABLE `a_ecif_etl_date` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `oc_date` varchar(45) NOT NULL,
  `update_datetime` datetime NOT NULL,
  `target_date` varchar(45) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for a_ecif_etl_parameter
-- ----------------------------
DROP TABLE IF EXISTS `a_ecif_etl_parameter`;
CREATE TABLE `a_ecif_etl_parameter` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(45) NOT NULL,
  `value` varchar(100) NOT NULL,
  `update_datetime` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=20 DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for a_etl_jobinfo
-- ----------------------------
DROP TABLE IF EXISTS `a_etl_jobinfo`;
CREATE TABLE `a_etl_jobinfo` (
  `id` varchar(10) DEFAULT NULL COMMENT 'id',
  `rqbdate` date DEFAULT NULL COMMENT '批处理日期',
  `job_name` varchar(100) DEFAULT NULL COMMENT '作业名称',
  `begin_time` datetime DEFAULT NULL COMMENT '作业启动时间',
  `end_time` datetime DEFAULT NULL COMMENT '作业结束时间',
  `status` varchar(100) DEFAULT NULL COMMENT '作业运行情况 0-执行成功 1-正在执行 2-执行失败',
  `job_type` varchar(100) DEFAULT NULL COMMENT '作业类型',
  `job_code` int(100) DEFAULT NULL COMMENT '作业编码',
  `remark` text COMMENT '说明',
  UNIQUE KEY `rqbdate` (`rqbdate`,`job_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for a_etl_namespace
-- ----------------------------
DROP TABLE IF EXISTS `a_etl_namespace`;
CREATE TABLE `a_etl_namespace` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `rqbdate` datetime NOT NULL,
  `namespace` varchar(255) NOT NULL,
  `status` varchar(1) NOT NULL,
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for a_etl_rqb
-- ----------------------------
DROP TABLE IF EXISTS `a_etl_rqb`;
CREATE TABLE `a_etl_rqb` (
  `id` varchar(10) DEFAULT NULL COMMENT 'id',
  `rqbdate` date DEFAULT NULL,
  `status` varchar(10) DEFAULT NULL COMMENT '当前批处理状态 0-成功 1-正在执行 2-执行失败',
  `remark` varchar(500) DEFAULT NULL COMMENT '说明'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- ----------------------------
-- Table structure for merge_cust_history
-- ----------------------------
DROP TABLE IF EXISTS `merge_cust_history`;
CREATE TABLE `merge_cust_history` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` varchar(32) NOT NULL,
  `cust_id` varchar(32) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1946 DEFAULT CHARSET=utf8;
