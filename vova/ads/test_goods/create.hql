--商品维度汇总表
drop table ads.ads_test_goods_behave;
CREATE TABLE IF NOT EXISTS ads.ads_test_goods_behave
(
    datasource         STRING COMMENT '',
    platform           STRING COMMENT '平台',
    goods_id           BIGINT COMMENT '商品id',
    clicks             BIGINT COMMENT '点击数',
    impressions        BIGINT COMMENT '曝光数',
    sales_order        BIGINT COMMENT '商品销量',
    users              BIGINT COMMENT '点击uv',
    gmv                DOUBLE COMMENT '',
    ctr                DOUBLE COMMENT '',
    gcr                DOUBLE COMMENT '',
    created_time       timestamp COMMENT '',
    last_update_time   timestamp COMMENT ''
) COMMENT '测试集商品数据' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


 CREATE TABLE ads_test_cat_gcr_1w (
  first_cat_id int(11)  NOT NULL COMMENT '一级分类id',
  second_cat_id int(1)  NOT NULL COMMENT '二级分类id',
  is_brand int(4)  NOT NULL COMMENT '是否品牌',
  geo_country varchar(20) NOT NULL DEFAULT '0' COMMENT '国家',
  gcr decimal(10,4) NOT NULL DEFAULT '0' COMMENT 'gcr',
  PRIMARY KEY (first_cat_id,second_cat_id,is_brand,geo_country)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='测试集商品二级分类gcr';


drop table if exists tmp.ads_test_cat_gcr_res_1w;
create table tmp.ads_test_cat_gcr_res_1w as
select
first_cat_id,
nvl(second_cat_id,0) second_cat_id,
is_brand,
geo_country,
avg(gcr) gcr
from
tmp.ads_test_cat_gcr_1w
group by first_cat_id,second_cat_id,is_brand,geo_country;

drop table ads.ads_test_goods_h;
CREATE TABLE IF NOT EXISTS ads.ads_test_goods_h
(

) partitioned BY (pt string,hour string)  COMMENT '测试集商品数据' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table ads.ads_test_goods_h;
CREATE TABLE IF NOT EXISTS ads.ads_test_goods_h
(
   id INT,
   datasource STRING,
   platform STRING,
   region_codes STRING,
   region_ids STRING,
   goods_id INT,
   users BIGINT,
   clicks BIGINT,
   impressions BIGINT,
   sales_order BIGINT,
   is_compliance BIGINT,
   employee_name STRING,
   select_cat_channel STRING,
   gmv DECIMAL(10,4),
   ctr DECIMAL(10,4),
   gcr DECIMAL(10,4),
   test_status BIGINT,
   test_result BIGINT,
   create_time TIMESTAMP,
   last_update_time STRING
) COMMENT '测试集商品数据'
 partitioned BY (pt string,hour string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



drop table ads.ads_vova_test_goods_h;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_test_goods_h(
id INT,
datasource STRING,
platform STRING,
region_codes STRING,
region_ids STRING,
goods_id INT,
users BIGINT,
clicks BIGINT,
impressions BIGINT,
sales_order BIGINT,
is_compliance BIGINT,
employee_name STRING,
select_cat_channel STRING,
gmv DOUBLE,
ctr DOUBLE,
gcr DOUBLE,
test_status BIGINT,
test_result BIGINT,
create_time TIMESTAMP,
last_update_time STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
