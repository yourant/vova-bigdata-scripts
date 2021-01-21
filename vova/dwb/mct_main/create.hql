drop table dwb.dwb_main_report;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_main_report
(
    event_date      STRING COMMENT 'd_日期',
    mct_id          STRING COMMENT 'd_mct_id',
    mct_name        STRING COMMENT 'd_商家名',
    imp_uvs         BIGINT COMMENT 'i_店铺商品曝光uv',
    gmv             DECIMAL(10, 4) COMMENT 'i_gmv',
    order_nums      BIGINT COMMENT 'i_成功支付订单量',
    buyer_nums      BIGINT COMMENT 'i_成功支付uv',
    inac_gmv        DECIMAL(10, 4) COMMENT 'i_非活动gmv',
    inac_order_nums BIGINT COMMENT 'i_非活动支付成功订单量',
    inac_buyer_nums BIGINT COMMENT 'i_非活动成功支付uv',
    on_sale_gs      BIGINT COMMENT 'i_在架商品数',
    pdbuy_uv        BIGINT COMMENT '',
    pd_uv           BIGINT COMMENT '',
    cart_success_uv BIGINT COMMENT '',
    cart_uv         BIGINT COMMENT '',
    on_sale_dt      STRING COMMENT 'i_上新商品最新时间',
    on_sale_dt2     STRING COMMENT '',
    imp_pvs         BIGINT COMMENT 'i_店铺商品曝光量',
    spsor_name      STRING COMMENT 'd_招商名'
) COMMENT '商家主流程' PARTITIONED BY (pt string)
 STORED AS PARQUETFILE;

