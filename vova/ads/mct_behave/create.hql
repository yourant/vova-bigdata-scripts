-- 商家表现数据
CREATE TABLE `ads.ads_mct_behave_3m`(
  `mct_id` bigint COMMENT '店铺ID',
  `mct_name` string COMMENT '店铺英文名称',
  `confirm_order_cnt_3m` bigint,
  `confirm_order_cnt_1m` bigint,
  `order_cnt_1m` bigint,
  `gmv_1m` decimal(38,4),
  `refund_rate_9w` decimal(14,4),
  `wl_refund_rate_9w` decimal(14,4),
  `nwl_refund_rate_9w` decimal(14,4),
  `mct_cancel_cnt` bigint,
  `mct_cancel_rate` decimal(14,4),
  `mark_deliver_rate` decimal(14,4),
  `online_rate` decimal(14,4),
  `loss_weight_rate` decimal(14,4),
  `exp_income` double,
  `second_cat_ids` string)
 COMMENT '商家表现数据' PARTITIONED BY (pt string)  STORED AS PARQUETFILE;

 alter table ads.ads_mct_behave_3m add columns(year_refund_rate decimal(13,2) comment '年内退款率') cascade;
 alter table ads.ads_mct_behave_3m add columns(received_rate_9w decimal(13,2) comment '9周妥投退款率') cascade;
 alter table ads.ads_mct_behave_3m add columns(received_rate_year decimal(13,2) comment '年内妥投退款率') cascade;
 alter table ads.ads_mct_behave_3m add columns(delivered_time_per_60 decimal(13,2) comment '60分位物流时效') cascade;
 alter table ads.ads_mct_behave_3m add columns(delivered_time_per_80 decimal(13,2) comment '80分位物流时效') cascade;
 alter table ads.ads_mct_behave_3m add columns(delivered_time_per_90 decimal(13,2) comment '90分位物流时效') cascade;


 CREATE TABLE IF NOT EXISTS themis.ads_mct_behave_3m(
 \`id\`                          int(11)        NOT NULL AUTO_INCREMENT          COMMENT    '自增主键',
 \`pt\`                          varchar(128)    COMMENT    '日期时间字符串',
 \`mct_id\`                      int             COMMENT    '店铺id',
 \`mct_name\`                    varchar(128)    COMMENT    '店铺名称',
 \`confirm_order_cnt_3m\`        int             COMMENT    '近90天确任子订单数',
 \`confirm_order_cnt_1m\`        int             COMMENT    '近30天确任子订单数',
 \`order_cnt_1m\`                int             COMMENT    '近30天子订单数',
 \`gmv_1m\`                      decimal(10,2)   COMMENT    '近30天店铺GMV',
 \`refund_rate_9w\`              decimal(10,2)   COMMENT    '9周退款率',
 \`wl_refund_rate_9w\`           decimal(10,2)   COMMENT    '9周物流退款率',
 \`nwl_refund_rate_9w\`          decimal(10,2)   COMMENT    '9周非物流退款率',
 \`mct_cancel_cnt\`              int             COMMENT    '商家取消数',
 \`mct_cancel_rate\`             decimal(10,2)   COMMENT    '商家取消率',
 \`mark_deliver_rate\`           decimal(10,2)   COMMENT    '标记发货率',
 \`online_rate\`                 decimal(10,2)   COMMENT    '上网率',
 \`loss_weight_rate\`            decimal(10,2)   COMMENT    '重量缺失率',
 \`exp_income\`                  decimal(10,2)   COMMENT    '店铺曝光效率',
 \`second_cat_ids\`              varchar(128)    COMMENT    '二级品类top3',
 \`last_update_time\`            timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (\`id\`) USING BTREE,
 UNIQUE KEY \`mct_id_pr_key\` (\`pt\`,\`mct_id\`) USING BTREE,
 UNIQUE KEY \`mct_name_pr_key\` (\`pt\`,\`mct_name\`) USING BTREE
 )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='商家表现数据';
