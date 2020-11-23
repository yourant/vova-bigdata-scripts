CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_ok_coupon_arc
(
  `coupon_id` bigint COMMENT '红包ID',
  `party_id` bigint,
  `coupon_config_id` bigint COMMENT '红包配置Id',
  `coupon_code` string COMMENT '红包 code',
  `coupon_ctime` bigint COMMENT '红包生成时间, create time',
  `extend_day` bigint COMMENT 'coupon生成后的有效时间段，以天为单位',
  `extend_second` bigint COMMENT 'coupon生成后的有效时间段，以秒为单位',
  `coupon_cip` bigint COMMENT '红包生成的ip, create ip',
  `coupon_status` bigint COMMENT '红包状态',
  `coupon_comment` string COMMENT '备注',
  `coupon_gtime` bigint COMMENT '发红包时间',
  `coupon_creator` bigint COMMENT '红包创建人',
  `can_use_times` bigint COMMENT '此红包最多可以使用的次数',
  `coupon_project` string COMMENT '可使用的网站',
  `used_times` bigint COMMENT '此红包已经使用的次数',
  `draw_timestamp` bigint COMMENT '红包领用时间',
  `coupon_applicant` string COMMENT '红包申请人',
  `user_id` bigint COMMENT '领用人用户ID，为空则表示未认领',
  `refer_id` string COMMENT '关联的订单',
  `give_user_id` bigint COMMENT '获取的这个红包的用户；发放人',
  `give_comment` string COMMENT '获取这个红包的备注',
  `give_time` bigint COMMENT '获取红包的时间',
  `used_timestamp` bigint COMMENT '使用时间',
  `used_order_id` bigint COMMENT '使用订单',
  `used_user_id` bigint COMMENT '使用人ID',
  `coupon_use_remarks` string COMMENT '红包备注文案'
 )comment '红包'
PARTITIONED BY (pt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


INSERT overwrite table ods_fd_vb.ods_fd_ok_coupon_arc PARTITION (pt='${hiveconf:pt}')
select  
    coupon_id,
    party_id,
    coupon_config_id,
    coupon_code,
    coupon_ctime,
    extend_day,
    extend_second,
    coupon_cip,
    coupon_status,
    coupon_comment,
    coupon_gtime,
    coupon_creator,
    can_use_times,
    coupon_project,
    used_times,
    draw_timestamp,
    coupon_applicant,
    user_id,
    refer_id,
    give_user_id,
    give_comment,
    give_time,
    used_timestamp,
    used_order_id,
    used_user_id,
    coupon_use_remarks
from tmp.tmp_fd_ok_coupon_full;
