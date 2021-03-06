drop table dwd.dwd_vova_fact_logistics;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_vova_fact_logistics
(
    datasource               string comment '数据平台',
    tracking_detail_id       bigint COMMENT '物流明细ID',
    tracking_id              bigint COMMENT '物流ID',
    order_goods_id           bigint COMMENT '子订单号',
    combine_id               string COMMENT '集运订单合包ID',
    collection_plan_id       bigint COMMENT '集货计划id',
    shipping_carrier_id      bigint COMMENT '物流方式ID',
    shipping_carrier_name    string COMMENT '物流方式名字',
    create_time              timestamp COMMENT '物流创建时间',
    first_update_date        string COMMENT '物流信息第一次记录时间',
    valid_tracking_date      string COMMENT '物流信息第一次记录有效时间',
    process_tag              string COMMENT 'aftership物流状态',
    shipment_type            string COMMENT '快递类型',
    error_code               string COMMENT '创建追踪时异常信息',
    tracking_status          string COMMENT '0:发货,1:运单已追踪,2:webhook推送有物流信息,3:异常',
    tracking_time            timestamp COMMENT '创建追踪时间',
    first_tracking_time      timestamp COMMENT '第一次追踪时间',
    source_id                string COMMENT '0:初始值,1:aftership,2:trackingmore,3:sunyou',
    origin_country           string COMMENT '始发国',
    destination_country      string COMMENT '目的国',
    service_type             string COMMENT '物流服务类型，即物流小分类对应的名称',
    weight                   DECIMAL(10, 4) COMMENT '重量',
    recipient_name           string COMMENT '收货人姓名',
    pickup_status            string COMMENT 'pickup揽货状态',
    is_in_collection         string COMMENT '是否入库（直发/集货仓）',
    in_collection_weight     DECIMAL(10, 4) COMMENT '入库重量，单位为KG',
    in_collection_time       timestamp COMMENT '入库时间',
    is_out_collection        string COMMENT '是否出库（直发/集货仓）',
    out_collection_weight    DECIMAL(10, 4) COMMENT '出库重量，单位为KG',
    out_collection_time      timestamp COMMENT '出库时间',
    is_delivered             bigint COMMENT '是否妥投（直发/尾程）',
    delivered_time           timestamp COMMENT '妥投时间(有物流妥投记物流妥投，没有记顾客签收时间)',
    delivered_date           date COMMENT '物流轨迹返回的妥投时间未格式化',
    custom_delivered_time    timestamp COMMENT '顾客妥投时间',
    logistic_delivered_time  timestamp COMMENT '物流妥投时间',
    confirm_time             timestamp COMMENT '确认订单时间',
    collecting_time          timestamp COMMENT '集运订单的商家标记发货时间',
    shipping_time            timestamp COMMENT '发货时间',
    shipping_abnormal_status bigint COMMENT '物流异常状态: 0.正常, 1.异常, 2.后台撤销, 3.申诉成功撤销, 4.已修改运单',
    sku_collecting_status    bigint COMMENT '退货状态0未退货/无需退货1待退货/需退货2退货中/退货物流进行中3退货收货仓已签收4签收合格/退货完成',
    shipping_tracking_number string COMMENT '运单号',
) COMMENT '物流事实表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
