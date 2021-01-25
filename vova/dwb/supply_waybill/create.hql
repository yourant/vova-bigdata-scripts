DROP TABLE dwb.dwb_vova_supply_waybill;
CREATE External TABLE IF NOT EXISTS dwb.dwb_vova_supply_waybill
(
    event_date          date COMMENT 'd_订单确认日期',
    mct_name            string COMMENT 'd_店铺名',
    region_code         string COMMENT 'd_国家',
    carrier_code        string COMMENT 'd_物流承运商',
    freight_range       string COMMENT 'd_运单成本区间',
    weight_range        string COMMENT 'd_尾程运单重量区间',
    waybill_cnt         bigint COMMENT 'i_运单量',
    order_goods_cnt     bigint COMMENT 'i_关联订单数',
    gmv                 DECIMAL(15, 2) COMMENT 'i_应付收入',
    order_amount        DECIMAL(15, 2) COMMENT 'i_实付收入',
    purchase_amount     DECIMAL(15, 2) COMMENT 'i_采购成本',
    shipping_fee        DECIMAL(15, 2) COMMENT 'i_运费项收入',
    freight             DECIMAL(15, 2) COMMENT 'i_运费成本',
    profit1             DECIMAL(15, 4) COMMENT 'i_揽收后损益①',
    profit2             DECIMAL(15, 4) COMMENT 'i_揽收后损益②',
    shipping_profit     DECIMAL(15, 4) COMMENT 'i_运费项损益',
    avg_profit1         DECIMAL(15, 4) COMMENT 'i_单均揽收后损益①',
    avg_profit2         DECIMAL(15, 4) COMMENT 'i_单均揽收后损益②',
    avg_shipping_profit DECIMAL(15, 4) COMMENT 'i_单均运费项损益',
    tot_waybill_cnt     bigint COMMENT 'i_当日总运单量',
    waybill_rate        DECIMAL(15, 4) COMMENT 'i_运单量占比'
) COMMENT '供应链运费分析报表（运单维度）'
    PARTITIONED BY ( pt string)  STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_supply_waybill_order;
CREATE External TABLE IF NOT EXISTS dwb.dwb_vova_supply_waybill_order
(
    event_date               date COMMENT 'd_订单确认日期',
    mct_name                 string COMMENT 'd_店铺名',
    region_code              string COMMENT 'd_国家',
    carrier_code             string COMMENT 'd_物流承运商',
    order_goods_amount_range string COMMENT 'd_订单价值区间',
    goods_amount_range       string COMMENT 'd_商品-销售价格区间',
    weight_range2             string COMMENT 'd_订单重量区间',
    first_cat_name           string COMMENT 'd_供应链商品pdd一级分类',
    second_cat_name          string COMMENT 'd_供应链商品pdd二级分类',
    three_cat_name           string COMMENT 'd_供应链商品pdd三级分类',
    valid_purchase           string COMMENT 'd_该订单是否采购成功',
    order_goods_cnt          bigint COMMENT 'i_订单量',
    gmv                      DECIMAL(15, 2) COMMENT 'i_应付收入',
    order_amount             DECIMAL(15, 2) COMMENT 'i_实付收入',
    purchase_amount          DECIMAL(15, 2) COMMENT 'i_采购成本',
    shipping_fee             DECIMAL(15, 2) COMMENT 'i_运费项收入',
    freight                  DECIMAL(15, 2) COMMENT 'i_运费成本',
    profit1                  DECIMAL(15, 4) COMMENT 'i_揽收后损益①',
    profit2                  DECIMAL(15, 4) COMMENT 'i_揽收后损益②',
    shipping_profit          DECIMAL(15, 4) COMMENT 'i_运费项损益',
    avg_profit1              DECIMAL(15, 4) COMMENT 'i_单均揽收后损益①',
    avg_profit2              DECIMAL(15, 4) COMMENT 'i_单均揽收后损益②',
    avg_shipping_profit      DECIMAL(15, 4) COMMENT 'i_单均运费项损益',
    tot_order_goods_cnt      bigint COMMENT 'i_总订单量',
    order_goods_rate         DECIMAL(15, 4) COMMENT 'i_订单量占比'
) COMMENT '供应链运费分析报表（子订单维度）'
    PARTITIONED BY ( pt string)  STORED AS PARQUETFILE;


DROP TABLE dwb.dwb_vova_supply_waybill_order_loss;
CREATE External TABLE IF NOT EXISTS dwb.dwb_vova_supply_waybill_order_loss
(
    event_date               date COMMENT 'd_订单确认日期',
    mct_name                 string COMMENT 'd_店铺名',
    region_code              string COMMENT 'd_国家',
    carrier_code             string COMMENT 'd_物流承运商',
    order_goods_amount_range string COMMENT 'd_订单价值区间',
    goods_amount_range       string COMMENT 'd_商品-销售价格区间',
    weight_range2             string COMMENT 'd_订单重量区间',
    first_cat_name           string COMMENT 'd_供应链商品pdd一级分类',
    second_cat_name          string COMMENT 'd_供应链商品pdd二级分类',
    three_cat_name           string COMMENT 'd_供应链商品pdd三级分类',
    valid_purchase           string COMMENT 'd_该订单是否采购成功',
    shipping_loss_amount_range string COMMENT 'd_运费亏损订单亏损区间',
    order_goods_cnt          bigint COMMENT 'i_订单量',
    gmv                      DECIMAL(15, 2) COMMENT 'i_应付收入',
    order_amount             DECIMAL(15, 2) COMMENT 'i_实付收入',
    purchase_amount          DECIMAL(15, 2) COMMENT 'i_采购成本',
    shipping_fee             DECIMAL(15, 2) COMMENT 'i_运费项收入',
    freight                  DECIMAL(15, 2) COMMENT 'i_运费成本',
    profit1                  DECIMAL(15, 4) COMMENT 'i_揽收后损益①',
    profit2                  DECIMAL(15, 4) COMMENT 'i_揽收后损益②',
    shipping_profit          DECIMAL(15, 4) COMMENT 'i_运费项损益',
    avg_profit1              DECIMAL(15, 4) COMMENT 'i_单均揽收后损益①',
    avg_profit2              DECIMAL(15, 4) COMMENT 'i_单均揽收后损益②',
    avg_shipping_profit      DECIMAL(15, 4) COMMENT 'i_单均运费项损益',
    tot_order_goods_cnt      bigint COMMENT 'i_总订单量',
    order_goods_rate         DECIMAL(15, 4) COMMENT 'i_订单量占比'
) COMMENT '供应链运费分析报表（子订单维度）'
    PARTITIONED BY ( pt string) STORED AS PARQUETFILE;


