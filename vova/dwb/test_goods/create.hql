drop table dwb.dwb_vova_test_goods_result;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_test_goods_result
(
employee_name            string   COMMENT '员工姓名',
select_cat_channel       string   COMMENT '选品渠道',
region_codes             string   COMMENT '国家',
is_brand                 string   COMMENT '是否品牌',
platform                 string   COMMENT '平台',
first_cat_name           string   COMMENT '一级品类',
in_cnt                   bigint   COMMENT '测试中数量',
success_cnt              bigint   COMMENT '成功数量',
fail_cnt                 bigint   COMMENT '失败数量',
no_comp_cnt              bigint   COMMENT '不合规数量'
)COMMENT '运营测款成功结果表'
PARTITIONED BY ( pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_test_goods_succ_group;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_test_goods_succ_group (
employee_name string COMMENT '员工姓名',
select_cat_channel string COMMENT '选品渠道',
first_cat_name string COMMENT '一级品类',
is_brand string COMMENT '是否品牌',
expre BIGINT COMMENT '曝光量',
gmv DECIMAL (14, 4) COMMENT 'gmv',
cat_gmv DECIMAL (14, 4) COMMENT 'cat_gmv'
) COMMENT '运营测款成功分组表' PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_test_goods_succ_detail;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_test_goods_succ_detail (
employee_name string COMMENT '员工姓名',
select_cat_channel string COMMENT '选品渠道',
is_brand string COMMENT '是否品牌',
first_cat_name string COMMENT '一级品类',
goods_id BIGINT COMMENT '商品id',
expre BIGINT COMMENT '曝光量',
gmv DECIMAL (14, 4) COMMENT 'gmv',
gmv_expre DECIMAL (14, 4) COMMENT 'gmv_expre'
) COMMENT '运营测款成功明细表' PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_test_goods;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_test_goods (
cur_date string COMMENT '日期',
goods_id BIGINT COMMENT '商品id',
employee_name string COMMENT '员工姓名',
select_cat_channel string COMMENT '选品渠道',
first_cat_name string COMMENT '一级品类',
test_result BIGINT COMMENT '测款结果',
expre_1w BIGINT COMMENT '最近一周曝光量',
gmv DECIMAL (14, 4) COMMENT 'gmv',
cat_gmv DECIMAL (14, 4) COMMENT 'cat_gmv'
) COMMENT '运营测款商品表' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
