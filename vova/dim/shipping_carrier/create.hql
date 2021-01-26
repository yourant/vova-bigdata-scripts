drop table dim.dim_vova_shipping_carrier;
CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_vova_shipping_carrier
(
    datasource          string comment '数据平台',
    carrier_id         bigint COMMENT '物流方式ID',
    carrier_url        string COMMENT '物流方式URL',
    eng_name           string COMMENT '物流方式英文名',
    cn_name            string COMMENT '物流方式中文名',
    provider_id        bigint COMMENT '物流商ID',
    provider_name      string COMMENT '物流商名字',
    logistics_type     bigint COMMENT '物流类型',
    after_ship_slug    string COMMENT '',
    carrier_cat        bigint COMMENT '物流类别 1:快速类,2:标准类,3:经济类,4:海外仓类',
    vovapost_is_active bigint COMMENT '线上打单是否可用',
    tracking_source    bigint COMMENT ''
) COMMENT '物流方式维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;