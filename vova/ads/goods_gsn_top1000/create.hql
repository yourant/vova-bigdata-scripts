DROP TABLE IF EXISTS ads.ads_gsn_top1000;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_gsn_top1000
(
    `ctry`                          string          COMMENT '国家',
    `rec_page_code`                 string          COMMENT '页面来源',
    `gsn`                           string          COMMENT 'gsn',
    `gs_id`                         string          COMMENT 'goodsid',
    `price`                         decimal(13,2)   COMMENT '价格',
    `shipping_fee`                  decimal(13,2)   COMMENT '运费',
    `pv`                            bigint          COMMENT '曝光数',
    `gmv`                           decimal(13,2)   COMMENT 'gmv'
) COMMENT 'top1000gsn曝光表'
   PARTITIONED BY(pt string)  STORED AS PARQUETFILE;





