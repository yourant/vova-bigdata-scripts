低价会场报表
需求方及需求号：谢赫 ,5319
创建时间及开发人员：2020/08/06,陈凯
修改需求方及需求号：谢赫 ,5725
修改人及修改时间：陈凯, 2020/08/06

Drop table dwb.dwb_vova_markdown_goods_daily;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_markdown_goods_daily (
datasource            string   COMMENT 'd_datasource',
region_code           string   COMMENT 'd_国家/地区',
platform              string   COMMENT 'd_平台:all,android,ios',

impression_goods_cnt  bigint   COMMENT 'i_会场曝光商品总数量',
pay_goods_cnt         bigint   COMMENT 'i_售卖成功商品数量',

impression_low_price_original_goods_cnt bigint        COMMENT '曝光商品数量中合格id商品数量',
impression_ac_top_goods_cnt             bigint        COMMENT '曝光商品数量中AEtop款商品数量',
pay_low_price_original_goods_cnt        bigint        COMMENT '售卖成功商品数量中合格id商品数量',
pay_ac_top_goods_cnt                    bigint        COMMENT '售卖成功商品数量中AEtop款商品数量',
gmv                                     decimal(16,4) COMMENT '会场GMV',
low_price_original_gmv                  decimal(16,4) COMMENT '合格id商品GMV',
ae_top_gmv                              decimal(16,4) COMMENT 'AEtop款商品GMV'

) COMMENT '会场商品数据' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_daily/"
;


Drop table if EXISTS dwb.dwb_vova_markdown_order_daily;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_markdown_order_daily (
datasource            string   COMMENT 'd_datasource',
region_code           string   COMMENT 'd_国家/地区',
platform              string   COMMENT 'd_平台:all,android,ios',

markdown_impression_uv           bigint        COMMENT 'i_低价会场曝光UV',
morrow_markdown_impression_uv    bigint        COMMENT 'i_低价会场次日曝光uv',
dau                              bigint        COMMENT 'i_主流程DAU',
gmv                              decimal(16,4) COMMENT 'i_主流程GMV',
markdown_order_gmv               decimal(16,4) COMMENT 'i_会场订单GMV',
markdown_order_goods_gmv         decimal(16,4) COMMENT 'i_会场商品GMV',
markdown_order_uv                bigint        COMMENT 'i_会场下单uv',
markdown_pay_uv                  bigint        COMMENT 'i_会场支付成功uv',
not_first_pay_uv                 bigint        COMMENT 'i_当日非首次支付uv'
) COMMENT '会场表现Daily总表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_order_daily/"
;


Drop table dwb.dwb_vova_markdown_goods_sales;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_markdown_goods_sales (
datasource            string   COMMENT 'd_datasource',
region_code           string   COMMENT 'd_国家/地区',
platform              string   COMMENT 'd_平台:all,android,ios',

virtual_goods_id              string         COMMENT 'd_商品虚拟id',
goods_sn                      string         COMMENT 'i_GSN',
mct_name                      string         COMMENT 'i_店铺名称',
activity_select_name          string         COMMENT 'i_商品来源:(low_price_original)合格id/(ae_top_1000)AEtop款',
first_cat_name                string         COMMENT 'i_一级品类名称',
second_cat_name               string         COMMENT 'i_二级品类名称',
activity_start_time           string         COMMENT 'i_入选时间:合格id的活动时间',
selling_price                 decimal(16,4)  COMMENT 'i_售价',
markdown_impression_cnt       bigint         COMMENT 'i_会场曝光量',
markdown_goods_number         bigint         COMMENT 'i_会场内销量',
markdown_gmv                  decimal(16,4)  COMMENT 'i_会场GMV',
no_markdown_impression_cnt    bigint         COMMENT 'i_非会场曝光量',
no_markdown_goods_number      bigint         COMMENT 'i_非会场销量',
no_markdown_gmv               decimal(16,4)  COMMENT 'i_非会场gmv',
no_markdown_avg_price         decimal(16,4)  COMMENT 'i_未参与降价id的均价',
no_markdown_max_goods_number  bigint         COMMENT 'i_未参与降价id的最高销量'
) COMMENT '会场商品销量数据' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_sales/"
;

2021-01-22 历史数据迁移
dwb.dwb_vova_markdown_goods_daily

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_daily/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_daily/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_daily/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_markdown_goods_daily/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_markdown_goods_daily/  s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_daily

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_daily/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_daily/

msck repair table dwb.dwb_vova_markdown_goods_daily;
select * from dwb.dwb_vova_markdown_goods_daily limit 20;

#
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_daily/pt=2021-0*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_markdown_goods_daily/pt=2021-01-22  s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_daily/pt=2021-01-22
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_markdown_goods_daily/pt=2021-01-23  s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_daily/pt=2021-01-23
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_markdown_goods_daily/pt=2021-01-24  s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_daily/pt=2021-01-24



@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
dwb.dwb_vova_markdown_order_daily

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_order_daily/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_order_daily/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_order_daily/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_markdown_order_daily/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_markdown_order_daily/  s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_order_daily

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_order_daily/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_order_daily/

msck repair table dwb.dwb_vova_markdown_order_daily;
select * from dwb.dwb_vova_markdown_order_daily limit 20;

#
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_order_daily/pt=2021-0*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_markdown_order_daily/pt=2021-01-22  s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_order_daily/pt=2021-01-22
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_markdown_order_daily/pt=2021-01-23  s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_order_daily/pt=2021-01-23

@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
dwb.dwb_vova_markdown_goods_sales

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_sales/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_sales/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_sales/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_markdown_goods_sales/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_markdown_goods_sales/  s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_sales

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_sales/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_sales/

msck repair table dwb.dwb_vova_markdown_goods_sales;
select * from dwb.dwb_vova_markdown_goods_sales limit 20;

#
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_sales/pt=2021-0*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_markdown_goods_sales/pt=2021-01-22  s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_sales/pt=2021-01-22
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_markdown_goods_sales/pt=2021-01-23  s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_sales/pt=2021-01-23
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_markdown_goods_sales/pt=2021-01-24  s3://bigdata-offline/warehouse/dwb/dwb_vova_markdown_goods_sales/pt=2021-01-24

