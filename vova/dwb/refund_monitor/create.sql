DROP TABLE IF EXISTS dwb.dwb_vova_refund_monitor;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_refund_monitor
(
cur_date string COMMENT 'd_日期',
region_code string COMMENT 'd_国家',
refund_reason string COMMENT 'd_退款事由',
orde_cnt string COMMENT 'i_确认订单数', 
mark_suces_orde_cnt string COMMENT 'i_标记发货订单数',
suces_orde_cnt string COMMENT 'i_成功发货订单数', 
suces_orde_money string COMMENT 'i_成功发货订单额',
actual_order_rate string COMMENT 'i_实际发货率', 
refund_cnt string COMMENT 'i_退款申请次数', 
refund_pass_cnt string COMMENT 'i_退款通过次数',
refund_cnt_1 string COMMENT 'i_1次通过', 
refund_cnt_2 string COMMENT 'i_2次通过', 
refund_cnt_3 string COMMENT 'i_3次通过',
refund_cnt_4 string COMMENT 'i_4次通过', 
appeal_rate string COMMENT 'i_申诉率', 
appeal_pass_rate string COMMENT 'i_申诉通过率' 
) COMMENT '退款审核监控' PARTITIONED BY (pt STRING)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;;

DROP TABLE IF EXISTS dwb.dwb_vova_refund_week_rate;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_refund_week_rate
(
cur_date string COMMENT 'd_日期',
region_code string COMMENT 'd_国家',
refund_reason string COMMENT 'd_退款事由',
refund_4rate string COMMENT 'i_4周退款率', 
refund_6rate string COMMENT 'i_6周退款率',
refund_9rate string COMMENT 'i_9周退款率', 
refund_12rate string COMMENT 'i_12周退款率',
refund_15rate string COMMENT 'i_15周退款率'
) COMMENT '国家退款率' PARTITIONED BY (pt STRING)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


