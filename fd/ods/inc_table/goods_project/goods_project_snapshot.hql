CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_project (
        goods_id bigint,
        project_name string,
        goods_thumb string,
        img_type string COMMENT '默认图图片类型',
        shop_price decimal(15, 4) COMMENT '商品价格，按项目分',
        market_price decimal(15, 4),
        group_price decimal(15, 4) COMMENT '团购价格',
        last_update_time string COMMENT '最后更新时间',
        weekly_deal string COMMENT 'weekly deal时间json值',
        stick_time bigint COMMENT '商品置顶时间',
        is_on_sale bigint,
        is_delete bigint,
        is_display bigint,
        sales_threshold bigint,
        on_sale_time string COMMENT '上架时间'
)COMMENT '根据不同项目的缩略图确定商品是否显示'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_goods_project
select `(dt)?+.+` from ods_fd_vb.ods_fd_goods_project_arc where dt = '${hiveconf:dt}';
