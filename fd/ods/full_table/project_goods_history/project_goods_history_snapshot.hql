CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_project_goods_history(
        `goods_history_id` bigint ,
        `table_name` string COMMENT '表名',
        `project_name` string COMMENT '所属组织',
        `field_id_name` string COMMENT 'field_id对应的名称',
        `field_id` bigint,
        `field_name` string COMMENT '字段名',
        `old_value` string comment '原来的值',
        `new_value` string COMMENT '新值',
        `user_name` string COMMENT '用户名',
        `modify_time` string COMMENT '修改时间',
        `memo` string COMMENT '修改描述',
        `oper_type` string,
        `parent_id` bigint)
COMMENT '各组织商品信息变更表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_project_goods_history
select `(dt)?+.+`
from ods_fd_vb.ods_fd_project_goods_history_arc
where dt = '${hiveconf:dt}';
