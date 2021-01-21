DROP TABLE IF EXISTS dwb.dwb_vova_mct_limited;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_mct_limited
(
    `mct_cnt`                       bigint          COMMENT '限流店铺数量',
    `min_limit_end_time`            date            COMMENT '最早限流结束时间',
    `max_limit_end_time`            date            COMMENT '最晚限流结束时间',
    `gmv`                           decimal(13,2)   COMMENT '限流商家最近一个月销售总额',
    `goods_on_sale_cnt`             bigint          COMMENT '限流商家最近一个月总在架商品数量'
) COMMENT '屏蔽店铺表'
PARTITIONED BY ( pt string) STORED AS PARQUETFILE;