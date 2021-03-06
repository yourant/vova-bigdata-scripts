DROP TABLE dwb.dwb_vova_pay_refund_detail;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_pay_refund_detail
(
    pay_time              timestamp COMMENT '支付时间',
    create_time           timestamp COMMENT '退款创建时间',
    refund_id             bigint COMMENT '退款ID',
    order_goods_id        bigint COMMENT '子订单ID',
    refund_type_id        bigint COMMENT '退款原因ID',
    refund_type           string COMMENT '退款原因（一级）',
    refund_reason_type_id bigint COMMENT '退款原因ID（二级）',
    refund_reason         string COMMENT '退款原因（二级）',
    refund_amount         decimal(10, 4) COMMENT '该退款的子订单金额',
    bonus                 decimal(10, 4) COMMENT '该退款的折扣，负值',
    exec_refund_time      timestamp COMMENT '执行退款的时间',
    sku_shipping_status   bigint COMMENT '发货状态',
    order_tag             string COMMENT '活动标签',
    order_goods_tag       string COMMENT '子订单活动标签',
    region_code           string COMMENT 'region_code',
    platform              string COMMENT '平台',
    threshold_amount      decimal(10, 4) COMMENT '阈值金额',
    final_delivery_time   timestamp COMMENT '最晚交期时间',
    receive_time          timestamp COMMENT '财务收款时间',
    first_refund_time     timestamp COMMENT '买家首次退款时间',
    buyer_id              bigint COMMENT '买家id',
    gmv                   decimal(10, 4) COMMENT 'gmv',
    brand_id              bigint COMMENT 'brand_id',
    storage_type          string COMMENT 'storage_type',
    datasource            string COMMENT 'string',
    is_new_activate       string COMMENT '是否当日新激活',
    threshold             string,
    is_first_refund       string,
    shipping_status_note  string,
    over_delivery_days    string
) COMMENT '支付退款明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_pay_refund_detail/"
;

drop table dwb.dwb_vova_pay_refund_report;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_pay_refund_report
(
    action_date           date,
    region_code             string COMMENT 'region_code',
    activity                string,
    platform                string COMMENT '平台',
    threshold               string COMMENT '平台',
    is_first_refund          string COMMENT '首次退款',
    over_delivery_days          string COMMENT '交期相关',
    shipping_status_note          string COMMENT '物流状态',
    storage_type          string COMMENT '库存来源，1-普通来源，2-海外仓',
    datasource          string,
    is_new_activate          string,
    user_number          bigint,
    order_goods_number          bigint,
    refund_amount          DECIMAL(14, 4),
    refund_buyer_number          bigint,
    refund_order_number          bigint,
    gmv          DECIMAL(14, 4),
    refund_reason_order_1 bigint,
    refund_reason_order_2 bigint,
    refund_reason_order_3 bigint,
    refund_reason_order_4 bigint,
    refund_reason_order_5 bigint,
    refund_reason_order_6 bigint,
    refund_reason_order_7 bigint,
    refund_reason_order_8 bigint,
    refund_reason_order_9 bigint,
    refund_reason_order_10 bigint,
    refund_reason_order_11 bigint,
    refund_reason_order_12 bigint,
    refund_reason_order_13 bigint,
    customer_reason_order_1 bigint,
    customer_reason_order_2 bigint,
    customer_reason_order_3 bigint,
    customer_reason_order_4 bigint,
    customer_reason_order_5 bigint,
    customer_reason_order_6 bigint,
    customer_reason_order_7 bigint,
    customer_reason_order_8 bigint,
    customer_reason_order_9 bigint,
    customer_reason_order_10 bigint,
    refund_reason_gmv_1 DECIMAL(14, 4),
    refund_reason_gmv_2 DECIMAL(14, 4),
    refund_reason_gmv_3 DECIMAL(14, 4),
    refund_reason_gmv_4 DECIMAL(14, 4),
    refund_reason_gmv_5 DECIMAL(14, 4),
    refund_reason_gmv_6 DECIMAL(14, 4),
    refund_reason_gmv_7 DECIMAL(14, 4),
    refund_reason_gmv_8 DECIMAL(14, 4),
    refund_reason_gmv_9 DECIMAL(14, 4),
    refund_reason_gmv_10 DECIMAL(14, 4),
    refund_reason_gmv_11 DECIMAL(14, 4),
    refund_reason_gmv_12 DECIMAL(14, 4),
    refund_reason_gmv_13 DECIMAL(14, 4),
    customer_reason_gmv_1 DECIMAL(14, 4),
    customer_reason_gmv_2 DECIMAL(14, 4),
    customer_reason_gmv_3 DECIMAL(14, 4),
    customer_reason_gmv_4 DECIMAL(14, 4),
    customer_reason_gmv_5 DECIMAL(14, 4),
    customer_reason_gmv_6 DECIMAL(14, 4),
    customer_reason_gmv_7 DECIMAL(14, 4),
    customer_reason_gmv_8 DECIMAL(14, 4),
    customer_reason_gmv_9 DECIMAL(14, 4),
    customer_reason_gmv_10 DECIMAL(14, 4)
) COMMENT '退款报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_pay_refund_report/"
;

# 历史数据迁移
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_pay_refund_report/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_pay_refund_report/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_pay_refund_report/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_pay_refund_report/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_pay_refund_report/  s3://bigdata-offline/warehouse/dwb/dwb_vova_pay_refund_report

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_pay_refund_report/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_pay_refund_report/

msck repair table dwb.dwb_vova_pay_refund_report;
select * from dwb.dwb_vova_pay_refund_report limit 20;


#mysql TABLE
-- CREATE TABLE `rpt_pay_refund_report` (
--   `action_date` date NOT NULL COMMENT '事件发生日期',
--   `region_code` varchar(10) NOT NULL DEFAULT '',
--   `activity` varchar(20) NOT NULL DEFAULT '',
--   `platform` varchar(20) NOT NULL DEFAULT '',
--   `threshold` varchar(30) NOT NULL DEFAULT '',
--   `is_first_refund` varchar(10) NOT NULL DEFAULT '',
--   `over_delivery_days` varchar(50) NOT NULL DEFAULT '',
--   `shipping_status_note` varchar(20) NOT NULL DEFAULT '',
--   `storage_type` varchar(10) NOT NULL DEFAULT '',
--   `datasource` varchar(20) NOT NULL DEFAULT '',
--   `is_new_activate` varchar(5) NOT NULL DEFAULT '',
--   `user_number` int(11) NOT NULL DEFAULT '0',
--   `refund_amount` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `order_goods_number` int(11) NOT NULL DEFAULT '0',
--   `refund_buyer_number` int(11) NOT NULL DEFAULT '0',
--   `refund_order_number` int(11) NOT NULL DEFAULT '0',
--   `gmv` decimal(14,4) NOT NULL DEFAULT '0.00',
--     refund_reason_order_1 INT(11) DEFAULT '0',
--     refund_reason_order_2 INT(11) DEFAULT '0',
--     refund_reason_order_3 INT(11) DEFAULT '0',
--     refund_reason_order_4 INT(11) DEFAULT '0',
--     refund_reason_order_5 INT(11) DEFAULT '0',
--     refund_reason_order_6 INT(11) DEFAULT '0',
--     refund_reason_order_7 INT(11) DEFAULT '0',
--     refund_reason_order_8 INT(11) DEFAULT '0',
--     refund_reason_order_9 INT(11) DEFAULT '0',
--     refund_reason_order_10 INT(11) DEFAULT '0',
--     refund_reason_order_11 INT(11) DEFAULT '0',
--     refund_reason_order_12 INT(11) DEFAULT '0',
--     refund_reason_order_13 INT(11) DEFAULT '0',
--     customer_reason_order_1 INT(11) DEFAULT '0',
--     customer_reason_order_2 INT(11) DEFAULT '0',
--     customer_reason_order_3 INT(11) DEFAULT '0',
--     customer_reason_order_4 INT(11) DEFAULT '0',
--     customer_reason_order_5 INT(11) DEFAULT '0',
--     customer_reason_order_6 INT(11) DEFAULT '0',
--     customer_reason_order_7 INT(11) DEFAULT '0',
--     customer_reason_order_8 INT(11) DEFAULT '0',
--     customer_reason_order_9 INT(11) DEFAULT '0',
--     customer_reason_order_10 INT(11) DEFAULT '0',
--   `refund_reason_gmv_1` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `refund_reason_gmv_2` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `refund_reason_gmv_3` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `refund_reason_gmv_4` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `refund_reason_gmv_5` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `refund_reason_gmv_6` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `refund_reason_gmv_7` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `refund_reason_gmv_8` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `refund_reason_gmv_9` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `refund_reason_gmv_10` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `refund_reason_gmv_11` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `refund_reason_gmv_12` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `refund_reason_gmv_13` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `customer_reason_gmv_1` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `customer_reason_gmv_2` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `customer_reason_gmv_3` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `customer_reason_gmv_4` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `customer_reason_gmv_5` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `customer_reason_gmv_6` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `customer_reason_gmv_7` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `customer_reason_gmv_8` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `customer_reason_gmv_9` decimal(14,4) NOT NULL DEFAULT '0.00',
--   `customer_reason_gmv_10` decimal(14,4) NOT NULL DEFAULT '0.00',
--   PRIMARY KEY (`action_date`,`region_code`,`activity`,`platform`,
--                `threshold`,`is_first_refund`,`over_delivery_days`,
--                `storage_type`,`datasource`,`is_new_activate`,`shipping_status_note`)
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='rpt_refund_report';
