CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_points(
        `points_id` bigint COMMENT '自增id', 
        `points_number` bigint COMMENT '积分数量', 
        `user_id` bigint COMMENT '用户id', 
        `order_sn` string COMMENT '订单号', 
        `goods_id` bigint COMMENT '商品id', 
        `comment_id` bigint COMMENT '评论id', 
        `share_site` string COMMENT '分享平台', 
        `points_type` bigint COMMENT '获取/使用',
        `specific_way` string COMMENT '具体使用途径', 
        `record_time` string COMMENT '积分记录时间', 
        `comment` string COMMENT '备注', 
        `share_id` bigint COMMENT '', 
        `total` bigint COMMENT '')
        COMMENT 'vbridal库同步的积分表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_points
select `(dt)?+.+`
from ods_fd_vb.ods_fd_points_arc
where dt = '${hiveconf:dt}';
