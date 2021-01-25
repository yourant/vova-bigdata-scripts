drop table if exists dwb.dwb_vova_order_goods_life_cycle_cnt;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_order_goods_life_cycle_cnt
(
    ctry                  string COMMENT 'd_国家',
    order_cnt             BIGINT COMMENT 'i_订单量',
    pay_cnt               BIGINT COMMENT 'i_支付订单量',
    unconfirmed_cnt       BIGINT COMMENT 'i_未确认订单量',
    cancel_cnt            BIGINT COMMENT 'i_取消订单量',
    pre_warehouse_cnt     BIGINT COMMENT 'i_前置仓订单量',
    jiewang_cnt           BIGINT COMMENT 'i_捷网订单量',
    yanwen_cnt            BIGINT COMMENT 'i_燕文订单量',
    common_cnt            BIGINT COMMENT 'i_普通订单量'
) COMMENT '子订单数监控报表' PARTITIONED BY (pt STRING)
   STORED AS PARQUETFILE;


drop table if exists dwb.dwb_vova_order_goods_life_cycle_type;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_order_goods_life_cycle_type
(
    ctry                  STRING COMMENT 'd_国家',
    type                  STRING COMMENT 'd_类型',
    order_cnt             STRING COMMENT 'i_订单量',
    pay_cnt               STRING COMMENT 'i_支付订单量',
    confirm_cnt           BIGINT COMMENT 'i_确认订单量',
    cancel_cnt            BIGINT COMMENT 'i_取消订单量',
    unfilled_cnt          BIGINT COMMENT 'i_未发货',
    mark_cnt              BIGINT COMMENT 'i_已标记发货',
    reported_cnt          STRING COMMENT 'i_已预报入库',
    online_cnt            BIGINT COMMENT 'i_已上线',
    in_warehouse_cnt      STRING COMMENT 'i_已入库',
    push_warehouse_cnt    STRING COMMENT 'i_已推出库',
    fact_out_warehouse_cnt STRING COMMENT 'i_实际出库',
    delivered_cnt          BIGINT COMMENT 'i_已妥投',
    confirm_not_delivered_cnt BIGINT COMMENT 'i_确认收货但无妥投',
    not_confirm_not_delivered_cnt BIGINT COMMENT 'i_未妥投未确认收货',
    apply_refund_cnt       BIGINT COMMENT 'i_已申请退款',
    refunded_cnt           BIGINT COMMENT 'i_退款通过'
) COMMENT '子订单生命周期监控报表' PARTITIONED BY (pt STRING)
   STORED AS PARQUETFILE;


