-- 【数据】[4947]AC品类表现数据报表
-- 筛选项
-- 时间
-- 国家   默认展示all 展示top20国家   gb,fr,de,it,es,nl,pt,es,us,cs,pl,be,mx,si,ru,jp,br,tw,na,au
-- 品类   默认展示all all，各二级品类
--
--
-- search数据表现
--      仅展示，不做筛选        "search页数据
-- 不同二级品类对应同一一级品类数据相同"  search页数据   二级品类下所有商品加购按钮UV/商详页UV   二级品类下所有商品已付款UV/二级品类商详页UV
-- 时间   国家  一级品类    二级品类    一级品类点击uv    二级品类点击UV    商详页转化率  商详到支付转化率

AC数据报表-search数据表现

Drop table dwb.dwb_vova_second_cat_manifest;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_second_cat_manifest (
datasource           string    COMMENT 'd_数据源',
region_code          string    COMMENT 'd_国家',
first_cat_name       string    COMMENT 'i_一级品类',
second_cat_name      string    COMMENT 'd_二级品类',

search_first_cat_uv  bigint    COMMENT 'i_search页数据一级品类点击UV',
search_second_cat_uv bigint    COMMENT 'i_search页数据二级品类点击UV',
add_cart_uv          bigint    COMMENT 'i_二级品类下所有商品加购按钮UV',
pd_uv                bigint    COMMENT 'i_商详页UV',
pay_uv               bigint    COMMENT 'i_商品支付UV'
) COMMENT 'AC数据报表-search数据表现' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_second_cat_manifest/"
;

select *
from dwb.dwb_vova_second_cat_manifest
where region_code='all' and second_cat_name ='all'


2021-01-22 历史数据迁移
dwb.dwb_vova_second_cat_manifest

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_second_cat_manifest/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_second_cat_manifest/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_second_cat_manifest/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_second_cat_manifest/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_second_cat_manifest/  s3://bigdata-offline/warehouse/dwb/dwb_vova_second_cat_manifest

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_second_cat_manifest/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_second_cat_manifest/

msck repair table dwb.dwb_vova_second_cat_manifest;
select * from dwb.dwb_vova_second_cat_manifest limit 20;