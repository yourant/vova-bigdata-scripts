drop table dwb.dwb_vova_first_cat_report;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_first_cat_report
(
event_date         date     COMMENT '事件发生日期',
datasource         string   COMMENT 'vova|airyclub',
country            string   COMMENT '国家',
is_activate        string   COMMENT '是否新激活',
is_fbv             string   COMMENT '是否海外仓',
main_channel       string   COMMENT '渠道',
first_cat_name     string   COMMENT '一级类目',
expres             bigint   COMMENT '曝光数',
clks               bigint   COMMENT '点击数',
cart_uv            bigint   COMMENT '加购uv',
cart_success_uv    bigint   COMMENT '加购成功uv',
pd_uv              bigint   COMMENT '商详页uv'
)COMMENT '一级品类报表'
PARTITIONED BY ( pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
