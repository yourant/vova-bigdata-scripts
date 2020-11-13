CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_display_order_artemis_country_arc (
        `id` bigint,
        `goods_id` bigint COMMENT '商品id',
        `country_code` string  ,
        `project_name` string ,
        `platform` string comment '默认web',
        `impressions` bigint COMMENT '列表展示',
        `clicks` bigint COMMENT '列表点击',
        `users` bigint COMMENT '详情访问',
        `sales_order` bigint COMMENT '销量排序',
        `detail_add_cart` bigint COMMENT '商品详情页加车',
        `list_add_cart` bigint COMMENT '列表页加车',
        `checkout` bigint COMMENT '支付',
        `sales_order_in_7_days` bigint COMMENT '7天销售量',
        `virtual_sales_order` bigint,
        `goods_order` bigint,
        `start_time` string,
        `end_time` string,
        `is_active` bigint,
        `last_update_time` string,
        `sales` bigint COMMENT '商品销量（即销售件数）'
        )
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


INSERT overwrite table ods_fd_vb.ods_fd_goods_display_order_artemis_country_arc PARTITION (dt='${hiveconf:dt}')
select id ,goods_id ,country_code ,project_name ,platform ,impressions ,clicks ,users ,sales_order ,detail_add_cart ,list_add_cart ,
      checkout ,sales_order_in_7_days ,virtual_sales_order ,goods_order ,start_time ,end_time ,is_active ,last_update_time ,sales
from (
    select id ,goods_id ,country_code ,project_name ,platform ,impressions ,clicks ,users ,sales_order ,detail_add_cart ,list_add_cart ,checkout ,sales_order_in_7_days ,virtual_sales_order ,goods_order ,start_time ,end_time ,is_active ,last_update_time ,sales,
        row_number () OVER (PARTITION BY id ORDER BY dt DESC) AS rank
    from(
        select
            '2020-01-01' as dt,
            id,
            goods_id,
            country_code,
            project_name,
            platform,
            impressions,
            clicks,
            users,
            sales_order,
            detail_add_cart,
            list_add_cart,
            checkout,
            sales_order_in_7_days,
            virtual_sales_order,
            goods_order,
            start_time,
            end_time,
            is_active,
            last_update_time,
            sales 
        from tmp.tmp_fd_goods_display_order_artemis_country_full

        UNION

        select
            '${hiveconf:dt}' as dt,
            id,
            goods_id,
            country_code,
            project_name,
            platform,
            impressions,
            clicks,
            users,
            sales_order,
            detail_add_cart,
            list_add_cart,
            checkout,
            sales_order_in_7_days,
            virtual_sales_order,
            goods_order,
            start_time,
            end_time,
            is_active,
            last_update_time,
            sales
        from ods_fd_vb.ods_fd_goods_display_order_artemis_country_inc where dt = '${hiveconf:dt}'
    )inc
) arc where arc.rank =1;
