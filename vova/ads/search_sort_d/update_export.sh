#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
echo "$pre_date"
fi
pre_month=`date -d "29 day ago ${pre_date}" +%Y-%m-%d`
echo "$pre_month"

sql="
drop table if exists themis.ads_search_sort_d_new;
drop table if exists themis.ads_search_sort_d_pre;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS themis.ads_search_sort_d_new (
  user_id int(11)  NOT NULL COMMENT '用户ID',
  first_cat_prefer_1w varchar(256) COMMENT '近7天一级品类偏好top10',
  second_cat_prefer_1w varchar(256) COMMENT '近7天二级品类偏好top10,如果不存在二级品类则取一级',
  second_cat_max_click_1m int(10)  COMMENT '近一个月点击最多二级品类，如果不存在二级品类则取一级',
  second_cat_max_collect_1m int(10) COMMENT '近一个月收藏最多二级品类，如果不存在二级品类则取一级',
  second_cat_max_cart_1m int(10)  COMMENT '近一个月加购最多二级品类，如果不存在二级品类则取一级',
  second_cat_max_order_1m int(10)  COMMENT '近一个月下单最多二级品类，如果不存在二级品类则取一级',
  brand_prefer_1w varchar(256)  COMMENT '近7天品牌偏好top10',
  brand_prefer_his varchar(256)  COMMENT '历史品牌偏好top10',
  brand_max_click_1m int(10)  COMMENT '近30天点击最多品牌',
  brand_max_collect_1m int(10)  COMMENT '近30天收藏最多品牌',
  brand_max_cart_1m int(10)  COMMENT '近30天加购最多品牌',
  brand_max_order_1m int(10)  COMMENT '近30天下单最多品牌',
  price_prefer_1w varchar(10)  COMMENT '近7天价格偏好层级',
  PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='搜索排序';

CREATE TABLE IF NOT EXISTS themis.ads_search_sort_d (
  user_id int(11)  NOT NULL COMMENT '用户ID',
  first_cat_prefer_1w varchar(256) COMMENT '近7天一级品类偏好top10',
  second_cat_prefer_1w varchar(256) COMMENT '近7天二级品类偏好top10,如果不存在二级品类则取一级',
  second_cat_max_click_1m int(10)  COMMENT '近一个月点击最多二级品类，如果不存在二级品类则取一级',
  second_cat_max_collect_1m int(10) COMMENT '近一个月收藏最多二级品类，如果不存在二级品类则取一级',
  second_cat_max_cart_1m int(10)  COMMENT '近一个月加购最多二级品类，如果不存在二级品类则取一级',
  second_cat_max_order_1m int(10)  COMMENT '近一个月下单最多二级品类，如果不存在二级品类则取一级',
  brand_prefer_1w varchar(256)  COMMENT '近7天品牌偏好top10',
  brand_prefer_his varchar(256)  COMMENT '历史品牌偏好top10',
  brand_max_click_1m int(10)  COMMENT '近30天点击最多品牌',
  brand_max_collect_1m int(10)  COMMENT '近30天收藏最多品牌',
  brand_max_cart_1m int(10)  COMMENT '近30天加购最多品牌',
  brand_max_order_1m int(10)  COMMENT '近30天下单最多品牌',
  price_prefer_1w varchar(10)  COMMENT '近7天价格偏好层级',
  PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='搜索排序';
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dmapreduce.map.memory.mb=8192 \
-Dmapreduce.reduce.memory.mb=8192 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--m 1 \
--table ads_search_sort_d_new \
--update-key "user_id" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_search_sort_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table themis.ads_search_sort_d to themis.ads_search_sort_d_pre,themis.ads_search_sort_d_new to themis.ads_search_sort_d;
EOF
echo "-------rename结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi