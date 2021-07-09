CREATE external TABLE if not exists ads.ads_fd_royalty_threshold_detail_d
(
  datasource        string          COMMENT 'datasource',
  cat_id            bigint          COMMENT '��ƷƷ��ID',
  region_code       string          COMMENT '����code',
  goods_id          bigint          COMMENT '��Ʒid',
  gmv               decimal(14,4)   COMMENT 'gmv'
) COMMENT '�����ֵ����ȡ��'
PARTITIONED BY (pt STRING)
LOCATION "s3://vova-mlb/REC/data/vova_fd_salary/fd/threshold_get/"
STORED AS PARQUETFILE;

CREATE external TABLE if not exists ads.ads_fd_royalty_threshold_d
(
  datasource                string      COMMENT 'datasource',
  cat_id                    bigint      COMMENT '��ƷƷ��ID',
  region_code               string      COMMENT '����code',
  month_sale_threshold      double      COMMENT '��������ֵ',
  rank_threshold            double      COMMENT '��Ʒ������ֵ'
) COMMENT '�����ֵ����ȡ��'
PARTITIONED BY (pt STRING)
LOCATION "s3://vova-mlb/REC/data/vova_fd_salary/fd/threshold_out/"
STORED AS PARQUETFILE;
