商品数据统计报表
需求方及需求号: 田晔, #3542
创建时间及开发人员：2020-03-23,廖保林

todo: 需要 com.vova.monitor.MonitorMain 发告警

drop table if exists dwb.dwb_vova_goods_gcr_gmv_report;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_goods_gcr_gmv_report
(
    event_date      string         COMMENT 'd_日期',
    datasource      string         COMMENT 'd_datasource',
    region_code     string         COMMENT 'd_国家',
    first_cat_name  string         COMMENT 'd_一级品类',
    second_cat_name string         COMMENT 'd_二级品类',
    is_brand        string         COMMENT 'd_是否品牌',
    rec_page_code   string         COMMENT 'd_展示页面',
    impression_pv   bigint         COMMENT 'i_曝光pv',
    impression_uv   bigint         COMMENT 'i_曝光uv',
    gmv             DECIMAL(14, 2) COMMENT 'i_gmv',
    click_pv        bigint         COMMENT 'i_点击pv',
    pay_uv          bigint         COMMENT 'i_pay_uv',
    gcr             bigint         COMMENT 'i_gcr',
    cart_uv         bigint         COMMENT 'i_cart_uv',
    cart_success_uv bigint         COMMENT 'i_cart_success_uv',
    ctr             DECIMAL(14, 4) COMMENT 'i_ctr',
    pay_uv_div_impression_uv          DECIMAL(14, 4) COMMENT 'i_转化率(支付成功uv/曝光uv)',
    cart_success_uv_div_impression_uv DECIMAL(14, 4) COMMENT 'i_加购成功率(加购成功uv/曝光uv)',
    third_cat_name  string         COMMENT 'd_三级品类',
    avg_shop_price  decimal(14,2)  COMMENT 'i_出单商品均价',
    paid_order_cnt  bigint         COMMENT 'i_子订单数',
    is_on_sale_cnt  bigint         COMMENT 'i_在架商品数'
) COMMENT '商品数据报表' PARTITIONED BY ( pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_goods_gcr_gmv_report/"
;


商品数据统计报表
2021-01-23 历史数据迁移

dwb.dwb_vova_goods_gcr_gmv_report

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_goods_gcr_gmv_report/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_goods_gcr_gmv_report/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_goods_gcr_gmv_report/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/goods_gcr_gmv_report/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/goods_gcr_gmv_report/  s3://bigdata-offline/warehouse/dwb/dwb_vova_goods_gcr_gmv_report

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_goods_gcr_gmv_report/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_goods_gcr_gmv_report/

msck repair table dwb.dwb_vova_goods_gcr_gmv_report;
select * from dwb.dwb_vova_goods_gcr_gmv_report limit 20;

#
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_goods_gcr_gmv_report/pt=2021-*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/goods_gcr_gmv_report/pt=2021-01-22  s3://bigdata-offline/warehouse/dwb/dwb_vova_goods_gcr_gmv_report/pt=2021-01-22

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/goods_gcr_gmv_report/pt=2021-01-23  s3://bigdata-offline/warehouse/dwb/dwb_vova_goods_gcr_gmv_report/pt=2021-01-23

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/goods_gcr_gmv_report/pt=2021-01-24  s3://bigdata-offline/warehouse/dwb/dwb_vova_goods_gcr_gmv_report/pt=2021-01-24
