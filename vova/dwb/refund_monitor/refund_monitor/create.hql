DROP TABLE IF EXISTS dwb.dwb_vova_refund_monitor_v2;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_refund_monitor_v2
(
    cur_date            string COMMENT '日期',
    region_code         string COMMENT '国家',
    refund_reason       string COMMENT '退款事由',
    orde_cnt            int COMMENT '确认订单数',
    mark_suces_orde_cnt int COMMENT '标记发货订单数',
    suces_orde_cnt      int COMMENT '成功发货订单数',
    suces_orde_money    decimal(15, 4) COMMENT '成功发货订单额',
    actual_order_rate   decimal(15, 2) COMMENT '实际发货率',
    refund_cnt          int COMMENT '退款申请次数',
    refund_pass_cnt     int COMMENT '退款通过次数',
    refund_cnt_1        decimal(15, 2) COMMENT '1次通过',
    refund_cnt_2        decimal(15, 2) COMMENT '2次通过',
    refund_cnt_3        decimal(15, 2) COMMENT '3次通过',
    refund_cnt_4        decimal(15, 2) COMMENT '4次通过',
    appeal_rate         decimal(15, 2) COMMENT '申诉率',
    appeal_pass_rate    decimal(15, 2) COMMENT '申诉通过率'
) COMMENT '退款审核监控' PARTITIONED BY (pt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
;

DROP TABLE IF EXISTS dwb.dwb_vova_refund_monitor_system;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_refund_monitor_system
(
    cur_date                 string COMMENT '日期',
    region_code              string COMMENT '国家',
    refund_reason            string COMMENT '退款事由',
    system_audit_cnt         int COMMENT '系统审核订单数',
    system_audit_passed_cnt  int COMMENT '系统通过订单数',
    system_audit_passed_rate decimal(15, 4) COMMENT '系统通过率',
    system_appeal_rate       decimal(15, 4) COMMENT '系统驳回申诉率',
    case_2                   int COMMENT '命中case2订单数',
    case_3                   int COMMENT '命中case3订单数',
    case_4                   int COMMENT '命中case4订单数',
    case_5                   int COMMENT '命中case5订单数',
    case_10                  int COMMENT '命中case10订单数',
    case_13                  int COMMENT '命中case13订单数',
    case_16                  int COMMENT '命中case16订单数',
    case_17                  int COMMENT '命中case17订单数',
    case_18                  int COMMENT '命中case18订单数',
    case_19                  int COMMENT '命中case19订单数',
    case_20                  int COMMENT '命中case20订单数',
    case_21                  int COMMENT '命中case21订单数',
    case_22                  int COMMENT '命中case22订单数',
    case_23                  int COMMENT '命中case23订单数',
    case_24                  int COMMENT '命中case24订单数',
    case_25                  int COMMENT '命中case25订单数'
) COMMENT '系统退款审核现状' PARTITIONED BY (pt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
;
