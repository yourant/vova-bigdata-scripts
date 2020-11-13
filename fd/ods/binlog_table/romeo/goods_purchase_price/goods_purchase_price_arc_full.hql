CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_goods_purchase_price_arc (
    goods_id                bigint comment '网站商品ID，非erp商品ID',
    provider_id             bigint comment '供应商的编号ID',
    dispatch_sn             string comment '商品工单号',
    price                   decimal(15, 4) comment '给供应商的价格',
    wrap_price              decimal(15, 4) comment '给供应商披肩的价格',
    is_delete               bigint comment '是否删除：1删除 0正常',
    ctime                   bigint comment '创建时间',
    pk_cat_id               bigint,
    last_purchase_price     string,
    last_purchase_wrapprice string,
    last_purchase_provider  string,
    provider_id2            bigint comment '供应商的编号ID',
    provider_id3            bigint comment '供应商的编号ID',
    provider_id4            bigint comment '供应商的编号ID',
    provider_id5            bigint comment '供应商的编号ID',
    ratio                   decimal(15, 4) comment 'provider_id 的分单比例',
    ratio2                  decimal(15, 4) comment 'provider_id2 的分单比例',
    ratio3                  decimal(15, 4) comment 'provider_id3 的分单比例',
    ratio4                  decimal(15, 4) comment 'provider_id4 的分单比例',
    ratio5                  decimal(15, 4) comment 'provider_id5 的分单比例',
    color                   string comment 'provider_id 的颜色分配',
    color2                  string comment 'provider_id2 的颜色分配',
    color3                  string comment 'provider_id3 的颜色分配',
    color4                  string comment 'provider_id4 的颜色分配',
    color5                  string comment 'provider_id5 的颜色分配'
) COMMENT '来自kafka erp订单每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_goods_purchase_price_arc PARTITION (dt = '${hiveconf:dt}')
select 
     goods_id, provider_id, dispatch_sn, price, wrap_price, is_delete, ctime, pk_cat_id, last_purchase_price, last_purchase_wrapprice, last_purchase_provider, provider_id2, provider_id3, provider_id4, provider_id5, ratio, ratio2, ratio3, ratio4, ratio5, color, color2, color3, color4, color5
from (

    select 
        dt,goods_id, provider_id, dispatch_sn, price, wrap_price, is_delete, ctime, pk_cat_id, last_purchase_price, last_purchase_wrapprice, last_purchase_provider, provider_id2, provider_id3, provider_id4, provider_id5, ratio, ratio2, ratio3, ratio4, ratio5, color, color2, color3, color4, color5,
        row_number () OVER (PARTITION BY goods_id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt,
                goods_id,
                provider_id,
                dispatch_sn,
                price,
                wrap_price,
                is_delete,
                f(ctime != "0000-00-00 00:00:00" or ctime is not null, unix_timestamp(ctime, "yyyy-MM-dd HH:mm:ss"),0) AS ctime,
                pk_cat_id,
                last_purchase_price,
                last_purchase_wrapprice,
                last_purchase_provider,
                provider_id2,
                provider_id3,
                provider_id4,
                provider_id5,
                ratio,
                ratio2,
                ratio3,
                ratio4,
                ratio5,
                color,
                color2,
                color3,
                color4,
                color5
        from tmp.tmp_fd_romeo_goods_purchase_price_full

        UNION

        select dt,goods_id, provider_id, dispatch_sn, price, wrap_price, is_delete, ctime, pk_cat_id, last_purchase_price, last_purchase_wrapprice, last_purchase_provider, provider_id2, provider_id3, provider_id4, provider_id5, ratio, ratio2, ratio3, ratio4, ratio5, color, color2, color3, color4, color5
        from (

            select  dt
                    goods_id,
                    provider_id,
                    dispatch_sn,
                    price,
                    wrap_price,
                    is_delete,
                    ctime,
                    pk_cat_id,
                    last_purchase_price,
                    last_purchase_wrapprice,
                    last_purchase_provider,
                    provider_id2,
                    provider_id3,
                    provider_id4,
                    provider_id5,
                    ratio,
                    ratio2,
                    ratio3,
                    ratio4,
                    ratio5,
                    color,
                    color2,
                    color3,
                    color4,
                    color5,
                    row_number () OVER (PARTITION BY goods_id ORDER BY event_id DESC) AS rank
            from ods_fd_romeo.ods_fd_romeo_goods_purchase_price_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
