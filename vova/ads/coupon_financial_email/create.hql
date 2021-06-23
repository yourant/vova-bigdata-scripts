-- 财务优惠券邮件
drop table if exists ads.ads_vova_coupon_financial_email;
create external table if  not exists  ads.ads_vova_coupon_financial_email (
    `datasource`                string   COMMENT '数据源',
    `region_code`               STRING   COMMENT '国家代码',
    `cpn_cfg_type_id`           int      COMMENT 'cpn_cfg_type_id',
    `cpn_cfg_type_name`         string   COMMENT '优惠券名称',
    `cpn_cfg_type`              string   COMMENT '优惠券类型',
    `currency`                  string   COMMENT '货币类型',
    `use_amount`                decimal(13,2)   COMMENT '优惠金额',
    `use_num`                   int      COMMENT '使用用户数',
    `use_user`                  int      COMMENT '使用用户数',
    `gmv`                       decimal(13,2)   COMMENT 'gmv',
    `coupon_rate`               decimal(13,2)   COMMENT 'coupon_rate'
)COMMENT '财务优惠券邮件' PARTITIONED BY (event_date string)
     STORED AS PARQUETFILE;
