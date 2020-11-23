drop table dim.dim_vova_coupon;
CREATE TABLE IF NOT EXISTS dim.dim_vova_coupon
(
    datasource       string comment '数据平台',
    cpn_id            BIGINT COMMENT '优惠券ID',
    cpn_code          string COMMENT '优惠券CODE',
    cpn_cfg_id        bigint COMMENT '优惠券配置ID',
    cpn_cfg_type      string COMMENT '优惠券配置使用方式 是值还是百分比',
    cpn_cfg_val       DECIMAL(10, 4) COMMENT '优惠券配置面值',
    cpn_use_type      string COMMENT '优惠券配置使用范围 是针对商品价格还是针对运费',
    cpn_cfg_type_id   bigint COMMENT '优惠券的类型 （促销，返现，补偿）',
    cpn_cfg_type_name string COMMENT '优惠券的类型名字',
    buyer_id          string COMMENT '优惠券所有者',
    cpn_create_time   timestamp COMMENT '优惠券创建时间',
    extend_day        BIGINT COMMENT '优惠券有效时长',
    can_use_times     BIGINT COMMENT '此红包最多可以使用的次数'
) COMMENT '优惠券维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

