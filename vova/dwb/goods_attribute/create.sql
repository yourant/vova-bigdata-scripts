drop table dwb.dwb_vova_goods_attribute;
CREATE EXTERNAL TABLE dwb.dwb_vova_goods_attribute
(
    cur_date               string COMMENT 'd_日期',
    region_code            string COMMENT 'd_国家',
    on_sale_goods          string COMMENT 'i_在售商品数',
    shield_cnt             string COMMENT 'i_禁售商品数',
    pay_goods              string COMMENT 'i_已售商品数',
    onsale_pay_goods       string COMMENT 'i_在售中已售商品数',
    on_sale_brand_goods    string COMMENT 'i_on_sale_brand_goods',
    on_sale_no_brand_goods string COMMENT 'i_on_sale_no_brand_goods'
)
    COMMENT '商品属性数据'
    PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;