CREATE external TABLE if not exists ads.ads_fd_royalty_threshold_detail_d
(
  datasource        string          COMMENT 'datasource',
  cat_id            bigint          COMMENT '商品品类ID',
  region_code       string          COMMENT '国家code',
  goods_id          bigint          COMMENT '商品id',
  gmv               decimal(14,4)   COMMENT 'gmv'
) COMMENT '提成阈值计算取数'
PARTITIONED BY (pt STRING)
LOCATION "s3://vova-mlb/REC/data/vova_fd_salary/fd/threshold_get/"
STORED AS PARQUETFILE;

CREATE external TABLE if not exists ads.ads_fd_royalty_threshold_d
(
  datasource                string      COMMENT 'datasource',
  cat_id                    bigint      COMMENT '商品品类ID',
  region_code               string      COMMENT '国家code',
  month_sale_threshold      double      COMMENT '月销额阈值',
  rank_threshold            double      COMMENT '商品序数阈值'
) COMMENT '提成阈值计算取数'
PARTITIONED BY (pt STRING)
LOCATION "s3://vova-mlb/REC/data/vova_fd_salary/fd/threshold_out/"
STORED AS PARQUETFILE;
