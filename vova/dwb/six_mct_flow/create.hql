drop table dwb.dwb_vova_six_mct_flow_monitor;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_six_mct_flow_monitor
(
    mct_name string comment '店铺名称',
    first_cat_name string COMMENT '一级类目名称',
    page string COMMENT '页面',
    expre_pv bigint COMMENT '商品曝光量',
    expre_uv bigint COMMENT '商品曝光人数',
    rp_expre_pv bigint COMMENT '扶持商品曝光量',
    rp_expre_uv bigint COMMENT '扶持商品曝光人数',
    rp_expre_rate string COMMENT '扶持流量比例',
    clk_pv bigint COMMENT '商品点击次数',
    clk_uv bigint COMMENT '商品点击人数',
    rp_clk_pv bigint COMMENT '扶持商品点击次数',
    rp_clk_uv bigint COMMENT '扶持商品点击人数',
    ctr string COMMENT 'ctr',
    rp_ctr string COMMENT '扶持商品ctr',
    order_cnt bigint COMMENT '订单数',
    pay_uv bigint COMMENT '支付人数',
    gmv decimal(13,2) COMMENT 'gmv',
    rp_order_cnt bigint COMMENT '扶持商品订单数',
    rp_pay_uv bigint COMMENT '扶持商品支付人数',
    rp_gmv decimal(13,2) COMMENT '扶持商品gmv',
    rp_gmv_rate string COMMENT '扶持gmv比例'
) COMMENT '6级店铺流量扶持效果-商家流量监控报表' PARTITIONED BY (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

drop table dwb.dwb_vova_six_mct_goods_rp_flow_monitor;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_six_mct_goods_rp_flow_monitor
(
    mct_name string COMMENT '店铺名称',
    page string COMMENT '页面',
    avg_expre_pv bigint COMMENT '单个商品平均流量',
    max_expre_pv bigint COMMENT '单个商品最高流量',
    min_expre_pv bigint COMMENT '单个商品最低流量',
    avg_gmv decimal(13,4) COMMENT '单个商品平均gmv',
    max_gmv decimal(13,4) COMMENT '单个商品最高gmv',
    expre_no_order_cnt bigint COMMENT '有曝光但未出单商品个数',
    expre_order_cnt bigint COMMENT '有曝光且出单商品个数'
) COMMENT '6级店铺流量扶持效果-商品扶持流量监控报表' PARTITIONED BY (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table dwb.dwb_vova_six_mct_block_reason;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_six_mct_block_reason
(
    mct_name string COMMENT '店铺名称',
    goods_id bigint COMMENT '商品id',
    block_reason string COMMENT '屏蔽理由'
) COMMENT '6级店铺流量扶持效果-商家/商品触发屏蔽情况报表' PARTITIONED BY (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

