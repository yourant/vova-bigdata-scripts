DROP TABLE dwb.dwb_vova_nps_email;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_nps_email
(
    nps_submit_time        timestamp COMMENT 'nps_submit_time',
    email                  string COMMENT 'email',
    order_sn               string COMMENT 'order_sn',
    rate                   string COMMENT 'rate',
    reason                 string COMMENT 'reason',
    order_type             string COMMENT 'order_type',
    order_time             timestamp COMMENT 'order_time',
    order_goods_cnt        bigint COMMENT 'order_goods_cnt',
    cancel_order_goods_cnt bigint COMMENT 'cancel_order_goods_cnt',
    ra_order_goods_cnt     bigint COMMENT 'ra_order_goods_cnt',
    ro_order_goods_cnt     bigint COMMENT 'ro_order_goods_cnt',
    fin_order_goods_cnt    bigint COMMENT 'fin_order_goods_cnt',
    buyer_level            string COMMENT 'buyer_level',
    min_pay_time           timestamp COMMENT 'min_pay_time',
    max_pay_time           timestamp COMMENT 'max_pay_time',
    his_gmv                decimal(18,2) COMMENT 'his_gmv',
    his_paid_order_cnt     bigint COMMENT 'his_paid_order_cnt',
    region_code            string COMMENT 'region_code'
) COMMENT 'dwb_vova_nps' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_nps;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_nps
(
    region_code      string COMMENT 'region_code',
    buyer_level      string COMMENT 'buyer_level',
    nps_rate_0_to_6  bigint COMMENT 'nps_rate_0_to_6',
    nps_rate_6_to_8  bigint COMMENT 'nps_rate_6_to_8',
    nps_rate_8_to_10 bigint COMMENT 'nps_rate_8_to_10',
    nps_rate_cnt     bigint COMMENT 'nps_rate_cnt',
    paid_cnt         bigint COMMENT 'paid_cnt'
) COMMENT 'dwb_vova_nps' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

sh /mnt/vova-bigdata-scripts/common/sqoop_import_themis_2.sh --db_code=vts --table_name=order_nps --mapers=2 --etl_type=ALL  --period_type=day --partition_num=3

