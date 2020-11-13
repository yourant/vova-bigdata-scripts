CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_points_arc(
        `points_id` bigint COMMENT '自增id', 
        `points_number` bigint COMMENT '积分数量', 
        `user_id` bigint COMMENT '用户id', 
        `order_sn` string COMMENT '订单号', 
        `goods_id` bigint COMMENT '商品id', 
        `comment_id` bigint COMMENT '评论id', 
        `share_site` string COMMENT '分享平台', 
        `points_type` int COMMENT '获取/使用', 
        `specific_way` string COMMENT '具体使用途径', 
        `record_time` string COMMENT '积分记录时间', 
        `comment` string COMMENT '备注', 
        `share_id` bigint COMMENT '', 
        `total` bigint COMMENT '')
        COMMENT 'vbridal库同步的积分表'
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_points_arc PARTITION (dt='${hiveconf:dt}')
select
    points_id,
    points_number,
    user_id,
    order_sn,
    goods_id,
    comment_id,
    share_site,
    points_type,
    specific_way,
    record_time,
    comment,
    share_id,
    total
from tmp.tmp_fd_points_full;

