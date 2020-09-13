CREATE TABLE `a_etl_illegal_certs` (
`id`  bigint(10) NOT NULL AUTO_INCREMENT COMMENT 'id' ,
`illegal_cert`  varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL DEFAULT NULL COMMENT '非法证件' ,
`status`  varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL DEFAULT NULL COMMENT '状态：Y 有效 N 失效' ,
`create_time`  datetime NULL DEFAULT NULL COMMENT '创建时间' ,
`update_time`  datetime NULL DEFAULT NULL COMMENT '更新时间' ,
PRIMARY KEY (`id`)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
