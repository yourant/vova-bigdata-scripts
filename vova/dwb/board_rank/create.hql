DROP TABLE dwb.dwb_vova_board_rank_v2;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_board_rank_v2
(
    event_date                   date           COMMENT 'd_事件发生日期',
    platform                     string         COMMENT 'd_platform',
    region_code                  string         COMMENT 'd_region_code',
    bod_dau                      bigint         COMMENT 'i_榜单首页和商品页UV去重',
    mkt_dau                      bigint         COMMENT 'i_大盘DAU',
    mkt_gmv                      decimal(14, 4) COMMENT 'i_大盘gmv',
    mkt_pay_user                 bigint         COMMENT 'i_大盘支付UV',
    mkt_pay_num                  bigint         COMMENT 'i_大盘支付订单数',
    bod_gmv                      decimal(14, 4) COMMENT 'i_仅统计榜单订单（不含其他渠道下单）',
    bod_order_gmv                decimal(14, 4) COMMENT 'i_仅统计榜单订单（同一订单内所有商品均有效）',
    bod_order_user               bigint         COMMENT 'i_榜单下单UV',
    bod_pay_user                 bigint         COMMENT 'i_榜单支付UV',
    bod_order_num                bigint         COMMENT 'i_榜单下单订单数',
    bod_pay_num                  bigint         COMMENT 'i_榜单支付订单数',
    bod_order_user_old           bigint         COMMENT 'i_',
    bod_pay_user_old             bigint         COMMENT 'i_',
    bod_order_user_new           bigint         COMMENT 'i_',
    bod_pay_user_new             bigint         COMMENT 'i_',
    bod_pay_again                bigint         COMMENT 'i_复购用户UV',
    bod_dau_b1                   bigint         COMMENT 'i_',
    bod_dau_cht_b1               bigint         COMMENT 'i_',
    bod_hmpg_uv                  bigint         COMMENT 'i_榜单首页UV',
    bod_hmpg_pv                  bigint         COMMENT 'i_榜单首页PV',
    bod_goods_uv                 bigint         COMMENT 'i_',
    bod_goods_pv                 bigint         COMMENT 'i_',
    bod_hmpg_list_1_3_uv         bigint         COMMENT 'i_',
    bod_hmpg_list_4_9_uv         bigint         COMMENT 'i_',
    bod_hmpg_list_10_15_uv       bigint         COMMENT 'i_',
    bod_hmpg_list_16_21_uv       bigint         COMMENT 'i_',
    bod_hmpg_list_22_27_uv       bigint         COMMENT 'i_',
    bod_hmpg_list_28_33_uv       bigint         COMMENT 'i_',
    bod_hmpg_list_34_39_uv       bigint         COMMENT 'i_',
    bod_hmpg_goods_click_rcmd_uv bigint         COMMENT 'i_',
    bod_hmpg_list_1_5_uv         bigint         COMMENT 'i_',
    bod_hmpg_list_1_5_pv         bigint         COMMENT 'i_',
    bod_hmpg_goods_rcmd_uv       bigint         COMMENT 'i_',
    bod_hmpg_goods_rcmd_pv       bigint         COMMENT 'i_',
    bod_goodspage_list_uv        bigint         COMMENT 'i_',
    bod_goodspage_list_pv        bigint         COMMENT 'i_',
    bod_goodspalace_rcmd_uv      bigint         COMMENT 'i_',
    bod_goodspalace_rcmd_pv      bigint         COMMENT 'i_',
    bod_goods_rcmd_uv            bigint         COMMENT 'i_',
    bod_goods_rcmd_pv            bigint         COMMENT 'i_',
    bod_goods_dtl_uv             bigint         COMMENT 'i_',
    bod_goods_dtl_pv             bigint         COMMENT 'i_',
    bod_no_brand_gmv             DECIMAL(14,4)  COMMENT 'i_',
    total_goods_num              BIGINT         COMMENT 'i_'
) COMMENT '榜单报表' PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;


