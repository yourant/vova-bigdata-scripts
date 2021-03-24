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



CREATE TABLE `test_goods_behave_test` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `datasource` varchar(32) NOT NULL COMMENT '来源vova|airyclub',
  `platform` varchar(32) NOT NULL,
  `region_ids` varchar(64) NOT NULL,
  `region_codes` varchar(64) NOT NULL,
  `goods_id` int(20) NOT NULL COMMENT '商品id',
  `clicks` bigint(20) NOT NULL DEFAULT '0' COMMENT '点击量',
  `impressions` bigint(20) NOT NULL DEFAULT '0' COMMENT '曝光量',
  `sales_order` bigint(20) NOT NULL DEFAULT '0' COMMENT '销量',
  `users` bigint(20) NOT NULL DEFAULT '0' COMMENT '点击uv',
  `gmv` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '成交额',
  `ctr` decimal(10,4) NOT NULL DEFAULT '0.0000' COMMENT 'ctr',
  `gcr` decimal(10,4) NOT NULL DEFAULT '0.0000' COMMENT 'gcr',
  `test_status` bigint(4) NOT NULL DEFAULT '0' COMMENT '状态',
  `test_result` bigint(4) NOT NULL DEFAULT '0' COMMENT '结果',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `last_update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `employee_name` varchar(64) NOT NULL DEFAULT '' COMMENT '员工姓名',
  `select_cat_channel` varchar(255) NOT NULL DEFAULT '' COMMENT '选品渠道',
  `is_compliance` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否合规, 0:无，1:合规 2;不合规',
  `rank` tinyint(3) unsigned DEFAULT '1' COMMENT '1~5 五个级别，用于指定测款优先级，值大的优先级高',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `datasource` (`datasource`,`goods_id`,`platform`,`region_codes`) USING BTREE,
  KEY `last_update_time` (`last_update_time`) USING BTREE,
  KEY `region_ids` (`region_ids`) USING BTREE,
  KEY `ix_ds_status` (`datasource`,`test_status`),
  KEY `create_time` (`create_time`),
  KEY `ix_goods_id` (`goods_id`,`region_codes`),
  KEY `select_cat_channel` (`select_cat_channel`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC


--商品维度汇总表

drop table ads.ads_vova_test_goods_behave_h;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_test_goods_behave_h
(
id                 bigint COMMENT '',
datasource         STRING COMMENT '数据源',
platform           STRING COMMENT '平台',
region_codes       STRING COMMENT '国家code',
region_ids         STRING COMMENT '国家ids',
goods_id           BIGINT COMMENT '商品id',
clicks             BIGINT COMMENT '点击数',
users              BIGINT COMMENT '点击uv',
impressions        BIGINT COMMENT '曝光数',
sales_order        BIGINT COMMENT '商品销量',
gmv                DOUBLE COMMENT '',
ctr                DOUBLE COMMENT '',
gcr                DOUBLE COMMENT '',
test_status        bigint COMMENT '',
test_result        bigint COMMENT '',
is_compliance      bigint COMMENT '',
select_cat_channel STRING COMMENT '',
employee_name      STRING COMMENT '',
create_time       timestamp COMMENT ''
) COMMENT '测试集商品数据'
 PARTITIONED BY (pt string,hour string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



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
