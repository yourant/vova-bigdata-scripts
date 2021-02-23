create external table if  not exists  ads.ads_vova_email_good_comment (
    `email`                    string          COMMENT 'd_email',
    `language_code`            string          COMMENT 'i_语言'
) COMMENT 'VOVA平台监管组TP邀好评数据需求' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;