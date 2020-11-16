CREATE TABLE IF NOT EXISTS dwb.dwb_fd_order_attribution_report (
`project_name` string COMMENT '组织',
`country` string COMMENT '国家',
`platform_type` string COMMENT '平台',
`page_code` string COMMENT 'page_code',
`list_type` string COMMENT 'list_type',
`goods_impression_cnt` BIGINT COMMENT '曝光数',
`goods_click_cnt` bigint COMMENT '点击数',
`goods_impression_uv_cnt` bigint COMMENT '曝光UV',
`goods_click_uv_cnt` bigint COMMENT '点击UV',
`goods_add_uv_cnt` bigint COMMENT '加购成功UV',
`total_order_cnt` bigint COMMENT '订单数量',
`total_success_order_cnt` bigint COMMENT '支付成功订单数',
`total_order_user_uv_cnt` bigint COMMENT '支付成功订单的用户',
`gmv` decimal(15,4) COMMENT 'gmv'
) COMMENT '订单归因指标数据'
PARTITIONED BY (`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

/* 将spark sql 生成的临时表*/
insert overwrite table dwb.dwb_fd_order_attribution_report partition (dt = '${hiveconf:dt}')
select
    project_name,
    country as country,
    platform_type,
    page_code,
    list_type,
    goods_impression_cnt,
    goods_click_cnt,
    goods_impression_uv_cnt,
    goods_click_uv_cnt,
    goods_add_uv_cnt,
    total_order_cnt,
    total_success_order_cnt,
    total_order_user_uv_cnt,
    gmv
from tmp.tmp_order_attribute_to_list_type
distribute by pmod(cast(rand()*1000 as int),1);

/* 执行成功删除数据 */
drop table tmp.tmp_order_attribute_to_list_type;
