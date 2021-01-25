drop table if exists dwb.dwb_vova_rec_report;
CREATE EXTERNAL TABLE dwb.dwb_vova_rec_report
(
    event_date                date COMMENT 'd_事件发生日期',
    datasource                string COMMENT 'd_vova|airyclub',
    country                   string COMMENT 'd_国家',
    os_type                   string COMMENT 'd_ios|android',
    rec_page_code             string COMMENT 'd_页面',
    page_code                 string COMMENT 'd_页面',
    list_type                 string COMMENT 'd_list_type',
    expres                    bigint COMMENT 'i_曝光数',
    clks                      bigint COMMENT 'i_点击数',
    clk_uv                    bigint COMMENT 'i_点击uv',
    expre_uv                  bigint COMMENT 'i_曝光uv',
    cart_uv                   bigint COMMENT 'i_加购uv',
    order_number              bigint COMMENT 'i_订单数',
    payed_number              bigint COMMENT 'i_支付单数',
    payed_uv                  bigint COMMENT 'i_支付uv',
    gmv                       decimal(15, 4) COMMENT 'i_gmv',
    cart_uv_div_expre_uv      decimal(15, 4) COMMENT 'i_加购率',
    payed_uv_div_expre_uv     decimal(15, 4) COMMENT 'i_支付转化率',
    gmv_mom                   decimal(15, 4) COMMENT 'i_gmv环比',
    payed_uv_div_expre_uv_mom decimal(15, 4) COMMENT 'i_支付转化率环比',
    cart_uv_div_expre_uv_mom  decimal(15, 4) COMMENT 'i_加车率环比',
    activate_time             string COMMENT 'd_激活时间',
    is_brand                  string COMMENT 'd_是否brand',
    brand_status              string
)
    COMMENT '归因报表'
    PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;

drop table if exists dwb.dwb_vova_rec_active_report;
CREATE EXTERNAL TABLE dwb.dwb_vova_rec_active_report
(
    event_date    date COMMENT 'd_日期',
    datasource    string COMMENT 'd_datasource',
    country       string COMMENT 'd_国家',
    os_type       string COMMENT 'd_平台',
    page_code     string COMMENT 'd_页面page_code',
    list_type     string COMMENT 'd_list_type',
    expres        bigint COMMENT 'i_曝光数',
    clks          bigint COMMENT 'i_点击数',
    clk_uv        bigint COMMENT 'i_点击uv',
    expre_uv      bigint COMMENT 'i_曝光uv',
    cart_uv       bigint COMMENT 'i_加购uv',
    order_number  bigint COMMENT 'i_订单数',
    payed_number  bigint COMMENT 'i_支付单数',
    payed_uv      bigint COMMENT 'i_支付uv',
    gmv           decimal(15, 4) COMMENT 'i_gmv',
    activate_time string COMMENT 'd_激活时间',
    payed_gds_num bigint COMMENT 'i_从活动场产生的商品销量（同一个SKU售卖多件，统计多个）',
    total_gmv     decimal(15, 4) COMMENT 'i_大盘gmv',
    page_uv       bigint COMMENT 'i_活动页面uv',
    element_type  string COMMENT 'd_element_type'
)
    COMMENT '活动订单归因报表'
    PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;


drop table if exists dwb.dwb_vova_rec_report_base_h;
CREATE EXTERNAL TABLE dwb.dwb_vova_rec_report_base_h
(
    hour             bigint COMMENT '小时',
    datasource       string COMMENT '项目',
    country          string COMMENT '国家',
    os_type          string COMMENT 'os',
    rec_page_code    string COMMENT '页面',
    clk              bigint COMMENT '点击数',
    expre            bigint COMMENT '曝光数',
    ctr              decimal(15, 4) COMMENT 'ctr',
    clk_uv           bigint COMMENT '点击uv',
    expre_uv         bigint COMMENT '曝光uv',
    cart_uv          bigint COMMENT '加购uv',
    cart_uv_expre_uv decimal(15, 4) COMMENT '加购率',
    ord_cnt          bigint COMMENT '订单数',
    pay_ord_cnt      bigint COMMENT '支付单数',
    pay_uv           bigint COMMENT '支付uv',
    pay_uv_expre_uv  decimal(15, 4) COMMENT '支付转化率',
    gmv              decimal(15, 4) COMMENT 'gmv'
)
    COMMENT '小时订单归因基础表'
    PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;

drop table if exists dwb.dwb_vova_rec_report_h;
CREATE EXTERNAL TABLE dwb.dwb_vova_rec_report_h
(
    event_date          timestamp COMMENT 'd_日期',
    hour                bigint COMMENT 'd_小时',
    datasource          string COMMENT 'd_datasource',
    country             string COMMENT 'd_国家',
    os_type             string COMMENT 'd_平台',
    rec_page_code       string COMMENT 'd_页面rec_page_code',
    clk                 bigint COMMENT 'i_点击数',
    expre               bigint COMMENT 'i_曝光数',
    ctr                 decimal(15, 4) COMMENT 'i_ctr',
    clk_uv              bigint COMMENT 'i_点击uv',
    expre_uv            bigint COMMENT 'i_曝光uv',
    cart_uv             bigint COMMENT 'i_加购uv',
    cart_uv_expre_uv    decimal(15, 4) COMMENT 'i_加购率',
    ord_cnt             bigint COMMENT 'i_订单数',
    pay_ord_cnt         bigint COMMENT 'i_支付单数',
    pay_uv              bigint COMMENT 'i_支付uv',
    pay_uv_expre_uv     decimal(15, 4) COMMENT 'i_支付转化率',
    gmv                 decimal(15, 4) COMMENT 'i_gmv',
    pay_uv_expre_uv_yoy decimal(15, 4) COMMENT 'i_支付转化率同比'
)
    COMMENT '小时订单归因报表'
    PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;