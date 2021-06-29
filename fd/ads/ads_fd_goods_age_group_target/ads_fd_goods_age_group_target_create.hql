create external table if not exists ads.ads_fd_goods_age_group_target
(
    goods_id         bigint     comment '商品ID',
    project          string     comment '组织',
    country          string     comment '国家',
    platform_type    string     comment '平台',
    age_group        string     comment '年龄分组',
    clicks           bigint     comment '点击UV',
    impressions      bigint     comment '曝光UV'
) comment '商品30天年龄层分布表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create external table if not exists tmp.goods_user_birthdays
(
    goods_id         bigint     comment '商品ID',
    project          string     comment '组织',
    country          string     comment '国家',
    platform_type    string     comment '平台',
    record_type      string     comment '记录类型',
    list_type        string     comment '列表类型',
    goods_uv         bigint     comment 'uv',
    age              bigint     comment '年龄',
    age_group        string     comment '年龄分组'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


