DROP TABLE IF EXISTS dwb.dwb_vova_ac_category_distribute;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_ac_category_distribute(
    `ctry`              STRING          COMMENT 'd_国家',
    `paltform`          STRING          COMMENT 'd_设备',
    `second_cat_name`   STRING          COMMENT 'd_二级品类名',
    `gmv`               DECIMAL(13,2)   COMMENT 'i_gmv',
    `sales_vol`         BIGINT          COMMENT 'i_销量'
)COMMENT 'AC-品类销量及gmv分布表' PARTITIONED BY (pt STRING)  STORED AS PARQUETFILE;


DROP TABLE IF EXISTS dwb.dwb_vova_ac_category_price_range_distribute;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_ac_category_price_range_distribute(
    `type`               STRING          COMMENT 'd_类别',
    `ctry`               STRING          COMMENT 'd_国家',
    `paltform`           STRING          COMMENT 'd_设备',
    `second_cat_name`    STRING          COMMENT 'd_二级品类名',
    `value_price_range1` STRING          COMMENT 'i_价格区[0,5]值',
    `value_price_range2` STRING          COMMENT 'i_价格区间（5,10]值',
    `value_price_range3` STRING          COMMENT 'i_价格区间（10,15]值',
    `value_price_range4` STRING          COMMENT 'i_价格区间（15,20]值',
    `value_price_range5` STRING          COMMENT 'i_价格区间（20,30]值',
    `value_price_range6` STRING          COMMENT 'i_价格区间（30,50]值',
    `value_price_range7` STRING          COMMENT 'i_价格区间（50,100]值',
    `value_price_range8` STRING          COMMENT 'i_价格区间（100,+]值'
)COMMENT 'AC-品类销量gmv价格区间分布' PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;