CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_project_inc (
            goods_id int,
            project_name string,
            goods_thumb string,
            img_type string COMMENT '默认图图片类型',
            shop_price double COMMENT '商品价格，按项目分',
            market_price double,
            group_price double COMMENT '团购价格',
            last_update_time string COMMENT '最后更新时间',
            weekly_deal string COMMENT 'weekly deal时间json值',
            stick_time int COMMENT '商品置顶时间', 
            is_on_sale int,
            is_delete int,
            is_display int,
            sales_threshold int,
            on_sale_time string COMMENT '上架时间'
)COMMENT '根据不同项目的缩略图确定商品是否显示'
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT OVERWRITE TABLE ods_fd_vb.ods_fd_goods_project_inc PARTITION (dt='${hiveconf:dt}')
select
        goods_id,
        project_name,
        goods_thumb,
        img_type,
        shop_price,
        market_price,
        group_price,
        last_update_time,
        weekly_deal,
        stick_time,
        is_on_sale,
        is_delete,
        is_display,
        sales_threshold,
        on_sale_time
from tmp.tmp_fd_goods_project where dt = '${hiveconf:dt}';
