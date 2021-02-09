CREATE  TABLE IF NOT EXISTS dwb.dwb_fd_category_data_analyze_rpt
(
    reporte_date  string comment '日期',
    project string comment '组织',
    category_name  string comment '品类',
    country  string comment '国家',
    impression_num  bigint comment '曝光量',
    advs_product_pv bigint comment '广告商品详情页pv',
    click_num   bigint comment '点击量',
    ctr_rate   decimal(15, 4)comment 'CTR',
    add_car_rate  decimal(15, 4) comment '加购率',
	sales_volume decimal(15, 4) comment '销售额',
	sales bigint comment '销量',
	order_numbers bigint comment '订单量',
	avg_order_fees decimal(15, 4) comment '单单价',
	link_order_rate  decimal(15, 4) comment '连单率,单笔销售满2件的订单数/总订单数',
	all_ctr_rate  decimal(15, 4) comment '整体转化率'
) COMMENT '品类数据分析表'
partitioned by (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS parquet;