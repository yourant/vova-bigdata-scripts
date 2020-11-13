CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_project_arc
(
            goods_id int,
            project_name string,
            goods_thumb string,
            img_type string,
            shop_price double,
            market_price double,
            group_price double,
            last_update_time string,
            weekly_deal string,
            stick_time int, 
            is_on_sale int,
            is_delete int,
            is_display int,
            sales_threshold int,
            on_sale_time string
)COMMENT '根据不同项目的缩略图确定商品是否显示'
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

INSERT overwrite table ods_fd_vb.ods_fd_goods_project_arc PARTITION (dt='${hiveconf:dt}')
select goods_id,project_name,goods_thumb,img_type,shop_price,market_price,group_price,last_update_time,
        weekly_deal,stick_time,is_on_sale,is_delete,is_display,sales_threshold,on_sale_time
from (
    select goods_id,project_name,goods_thumb,img_type,shop_price,market_price,group_price,last_update_time,
        weekly_deal,stick_time,is_on_sale,is_delete,is_display,sales_threshold,on_sale_time
        row_number () OVER (PARTITION BY goods_id ORDER BY dt DESC) AS rank
    from(
        select
            dt
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
        from ods_fd_vb.ods_fd_goods_project_arc where dt = '${hiveconf:dt_last}'

        UNION

        select
            dt
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
        from ods_fd_vb.ods_fd_goods_project_inc where dt = '${hiveconf:dt}'
    )inc
) arc where arc.rank =1;
