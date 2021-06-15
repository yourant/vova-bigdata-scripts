DROP TABLE dwb.dwb_vova_quality_control_core_tag;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_quality_control_core_tag
(
    `weekday`                            string              COMMENT '时间周期',
    `ctry`                               string              COMMENT 'd_国家',
    `not_pingyou_deliverd_rate`          decimal(13,2)       COMMENT 'i_非平邮订单交期内妥投率',
    `collect_deliverd_rate`              decimal(13,2)       COMMENT 'i_集运订单交期内妥投率',
    `deliverd_time_avg`                  decimal(13,2)       COMMENT 'i_已妥投的集运订单的平均妥投时长',
    `refund_rate`                        decimal(13,2)       COMMENT 'i_12周退款率',
    `complaint_rate`                     decimal(13,2)       COMMENT 'i_订单支付后90天的申诉率',
    `cancal_rate`                        decimal(13,2)       COMMENT 'i_订单支付后14天的取消率',
    `cb_rate`                            decimal(13,2)       COMMENT '订单支付后60天的cb率'
) COMMENT '质量品控核心指标报表' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

alter table dwb.dwb_vova_quality_control_core_tag add columns(`start_refund_rate` decimal(13,2)       COMMENT '订单支付后9周的退款发起率') cascade;

DROP TABLE dwb.dwb_vova_quality_control_refund;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_quality_control_refund
(
    `weekday`                            string              COMMENT '时间周期',
    `ctry`                               string              COMMENT 'd_国家',
    `refund_order_cnt`                   bigint              COMMENT 'i_退款子订单数',
    `refund_9w_rate`                     decimal(13,2)       COMMENT 'i_9周退款率',
    `refund_12w_rate`                    decimal(13,2)       COMMENT 'i_12周退款率',
    `refund_15w_rate`                    decimal(13,2)       COMMENT 'i_15周退款率',
    `refund_15w_lrf_rate`                decimal(13,2)       COMMENT 'i_物流退款率',
    `refund_15w_nlrf_rate`               decimal(13,2)       COMMENT 'i_非物流退款率',
    `complaint_cnt`                      bigint              COMMENT 'i_申述量',
    `complaint_rate`                     decimal(13,2)       COMMENT 'i_申述率',
    `complaint_delivered_out_time_rate`  decimal(13,2)       COMMENT 'i_物流超期占比',
    `cancal_rate`                        decimal(13,2)       COMMENT 'i_取消率',
    `platform_refund_15w_rate`           decimal(13,2)       COMMENT '15周平台退款率',
    `mct_refund_15w_rate`                decimal(13,2)       COMMENT '15周商家退款率'
) COMMENT '质量品控退款报表' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

alter table dwb.dwb_vova_quality_control_refund add columns(`start_refund_9w_rate` decimal(13,2)       COMMENT '订单支付后9周的退款发起率') cascade;
alter table dwb.dwb_vova_quality_control_refund add columns(`start_refund_9w_lrf_rate` decimal(13,2)       COMMENT '订单支付后9周的物流退款发起率') cascade;
alter table dwb.dwb_vova_quality_control_refund add columns(`start_refund_9w_nlrf_rate` decimal(13,2)       COMMENT '订单支付后9周的非物流退款发起率') cascade;
alter table dwb.dwb_vova_quality_control_refund add columns(`is_brand` string      COMMENT '是否品牌') cascade;


DROP TABLE dwb.dwb_vova_quality_control_delivered;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_quality_control_delivered
(
    `weekday`                            string              COMMENT '时间周期',
    `ctry`                               string              COMMENT 'd_国家',
    `delivered_rate_12w`                 decimal(13,2)       COMMENT 'i_12周妥投率',
    `collection_delivered_rate_12w`      decimal(13,2)       COMMENT 'i_集运12周妥投率',
    `not_col_not_pingyou_delivered_rate_12w`         decimal(13,2)       COMMENT 'i_非集运非平邮12周妥投率',
    `collection_delivered_time_avg`      decimal(13,2)       COMMENT 'i_集运12周妥投时效',
    `not_col_not_pingyou_delivered_time_avg`         decimal(13,2)       COMMENT 'i_非集运非平邮12周妥投时效'
) COMMENT '质量品控妥投报表' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;


DROP TABLE dwb.dwb_vova_quality_control_shipments;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_quality_control_shipments
(
    `weekday`                            string              COMMENT '时间周期',
    `ctry`                               string              COMMENT 'd_国家',
    `shipping_type`                      string              COMMENT 'i_物流渠道',
    `is_brand`                           string              COMMENT 'i_是否brand',
    `confirm_order_cnt`                  int                 COMMENT 'i_确认子订单量',
    `shipment_rate_5d`                   decimal(13,2)       COMMENT 'i_5天发货率',
    `online_rate_1w`                     decimal(13,2)       COMMENT 'i_七天上网率',
    `online_rate_2w`                     decimal(13,2)       COMMENT 'i_十四天上网率',
    `in_warehouse_rate_1w`               string              COMMENT 'i_七天入库率',
    `in_warehouse_rate_12d`              string              COMMENT 'i_十四天入库率'
) COMMENT '质量品控发货报表' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;