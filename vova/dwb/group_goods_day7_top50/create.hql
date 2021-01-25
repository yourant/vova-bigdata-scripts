[7182]7日Top50商品数据
https://zt.gitvv.com/index.php?m=task&f=view&taskID=27063

任务描述
需求链接：
https://docs.google.com/spreadsheets/d/1vPUM14fQCdW5Y-DwBZZ6qoHK_Tce1C_bYi8kCEw_MXE/edit#gid=0

# ads.ads_min_price_goods_h
ads.ads_vova_min_price_goods_h

# ads.ads_mct_rank
ads.ads_vova_mct_rank

7日Top50商品数据
dwb.dwb_vova_group_goods_day7_top50

Drop table dwb.dwb_vova_group_goods_day7_top50;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_group_goods_day7_top50 (
datasource            string         COMMENT 'd_datasource',
virtual_goods_id      string         COMMENT 'd_商品虚拟ID',
first_cat_id          string         COMMENT 'd_一级品类ID',
first_cat_name        string         COMMENT 'd_一级品类名称',
second_cat_id         string         COMMENT 'd_二级品类ID',
second_cat_name       string         COMMENT 'd_二级品类名称',
third_cat_id          string         COMMENT 'd_三级品类ID',
third_cat_name        string         COMMENT 'd_三级品类名称',
brand_name            string         COMMENT 'd_brand名',
gmv                   DECIMAL(14, 4) COMMENT 'i_商品近七天GMV',
goods_number          bigint         COMMENT 'i_商品近七天销量',
mct_id                string         COMMENT 'd_店铺ID',
mct_name              string         COMMENT 'd_店铺名称',
first_cat_rank        string         COMMENT 'd_一级类目等级',
is_on_sale            string         COMMENT 'd_目前在架状态'
) COMMENT '7日Top50商品数据' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_group_goods_day7_top50/"
;


7日Top50商品数据
2021-01-23 历史数据迁移

dwb.dwb_vova_group_goods_day7_top50

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_group_goods_day7_top50/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_group_goods_day7_top50/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_group_goods_day7_top50/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_group_goods_day7_top50/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_group_goods_day7_top50/  s3://bigdata-offline/warehouse/dwb/dwb_vova_group_goods_day7_top50

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_group_goods_day7_top50/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_group_goods_day7_top50/

msck repair table dwb.dwb_vova_group_goods_day7_top50;
select * from dwb.dwb_vova_group_goods_day7_top50 limit 20;




