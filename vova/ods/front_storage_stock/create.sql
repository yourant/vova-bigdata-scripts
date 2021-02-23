drop table if exists ods_vova_ext.ods_vova_front_storage_stock;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_vova_ext.ods_vova_front_storage_stock
(
    goods_id                           string,
    sku_id                             string,
    display_storage                   string
) COMMENT '前置仓库存'
   STORED AS PARQUETFILE;
