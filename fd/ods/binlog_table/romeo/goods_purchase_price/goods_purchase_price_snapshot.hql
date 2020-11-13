CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_goods_purchase_price (
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
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_goods_purchase_price
select `(dt)?+.+` from ods_fd_romeo.ods_fd_romeo_goods_purchase_price_arc where dt = '${hiveconf:dt}';
