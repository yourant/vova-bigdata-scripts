CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_goods_project_arc (
    `id` bigint comment 'id',
    `goods_id` bigint,
    `party_id` bigint comment '组织',
    `created_at` bigint comment '生成时间戳bigint',
    `updated_at` bigint comment '更新时间戳bigint',
    `deleted_at` string,
    `on_sale_time` string comment '上架时间',
    `on_sale_staff` string comment '上架人',
    `off_sale_time` string comment '下架时间',
    `shop_price` decimal(10,2) comment '商品价格，按项目分',
    `market_price` decimal(10,2),
    `is_tort` string comment '是否侵权 N :未侵权 Y：已侵权',
    `risk_level` string comment '风控等级：h_danger：一级,m_danger：二级,l_danger：三级,danger：四级,l_secure：五级,secure：六级',
    `is_on_sale` tinyint,
    `is_delete` tinyint,
    `is_display` tinyint,
    `virtual_goods_id` string comment '虚拟id',
    `goods_selector` string comment '选款人'
) COMMENT '商品组织信息表'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_goods_project_arc PARTITION (dt = '${hiveconf:dt}')
select 
     id, goods_id, party_id, created_at, updated_at, deleted_at, on_sale_time, on_sale_staff, off_sale_time, shop_price, market_price, is_tort, risk_level, is_on_sale, is_delete, is_display, virtual_goods_id, goods_selector
from (

    select 
        dt,id, goods_id, party_id, created_at, updated_at, deleted_at, on_sale_time, on_sale_staff, off_sale_time, shop_price, market_price, is_tort, risk_level, is_on_sale, is_delete, is_display, virtual_goods_id, goods_selector,
        row_number () OVER (PARTITION BY id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt,
                id,
                goods_id,
                party_id,
                if(created_at != '0000-00-00 00:00:00', unix_timestamp(created_at, "yyyy-MM-dd HH:mm:ss"), 0) AS created_at,
                if(updated_at != '0000-00-00 00:00:00', unix_timestamp(updated_at, "yyyy-MM-dd HH:mm:ss"), 0) AS updated_at,
                deleted_at,
                on_sale_time,
                on_sale_staff,
                off_sale_time,
                shop_price,
                market_price,
                is_tort,
                risk_level,
                is_on_sale,
                is_delete,
                is_display,
                virtual_goods_id,
                goods_selector
        from tmp.tmp_fd_dmc_goods_project_full

        UNION

        select dt,id, goods_id, party_id, created_at, updated_at, deleted_at, on_sale_time, on_sale_staff, off_sale_time, shop_price, market_price, is_tort, risk_level, is_on_sale, is_delete, is_display, virtual_goods_id, goods_selector
        from (

            select  dt,
                    id,
                    goods_id,
                    party_id,
                    created_at,
                    updated_at,
                    deleted_at,
                    on_sale_time,
                    on_sale_staff,
                    off_sale_time,
                    shop_price,
                    market_price,
                    is_tort,
                    risk_level,
                    is_on_sale,
                    is_delete,
                    is_display,
                    virtual_goods_id,
                    goods_selector,
                    row_number() OVER (PARTITION BY id ORDER BY event_id DESC) AS rank
            from ods_fd_dmc.ods_fd_dmc_goods_project_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
