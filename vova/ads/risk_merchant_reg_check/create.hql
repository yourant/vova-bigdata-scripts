-- 风险商家规则验证
drop table if exists ads.ads_vova_risk_merchant_reg_check;
create external table if  not exists  ads.ads_vova_risk_merchant_reg_check (
    `mct_id`                   int    COMMENT 'i_商家id',
    `mct_name`                 string COMMENT 'd_商家名称',
    `price_list`               string COMMENT 'd_价格list',
    `goods_cnt`                int    COMMENT 'd_商品数量',
    `same_price_rate`          decimal(13,2)    COMMENT 'd_价格相同商品比例'
) COMMENT '风险商家规则验证' PARTITIONED BY (pt string)
       STORED AS PARQUETFILE;
