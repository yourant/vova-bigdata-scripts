#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pt=$(date -d "-1 day" +%Y-%m-%d)
fi

echo "pt=$pt"
sql="
drop table if exists rec_themis.goods_id_country_behave_new;
drop table if exists rec_themis.goods_id_country_behave_pre;
"
mysql -h rec-themis.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwthemiswriter -pZ1OtPRLkrUIusK0EeYO9Xjha7a79oToz -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS rec_themis.goods_id_country_behave_new (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_id bigint(20) unsigned NOT NULL COMMENT '商品id',
  country varchar(32) NOT NULL DEFAULT '' COMMENT '国家',
  clicks bigint(20) NOT NULL DEFAULT '0' COMMENT '列表点击',
  impressions bigint(20) NOT NULL DEFAULT '0' COMMENT '列表展示',
  sales_order bigint(20) NOT NULL DEFAULT '0' COMMENT '销量',
  users bigint(20) NOT NULL DEFAULT '0' COMMENT '详情访问',
  impression_users bigint(20) NOT NULL DEFAULT '0' COMMENT '详情访问',
  gmv double NOT NULL DEFAULT '0.00' COMMENT '成交额',
  ctr double NOT NULL DEFAULT '0.0000' COMMENT 'ctr',
  gcr double NOT NULL DEFAULT '0.0000' COMMENT 'gcr',
  cr double NOT NULL DEFAULT '0.0000' COMMENT 'cr',
  click_cr double NOT NULL DEFAULT '0.0000' COMMENT 'click_cr',
  last_update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY unique_key (goods_id,country) USING BTREE,
  KEY country (country)USING BTREE,
  KEY sales_order (sales_order) USING BTREE,
  KEY gmv (gmv) USING BTREE,
  KEY ctr (ctr) USING BTREE,
  KEY gcr (gcr) USING BTREE,
  KEY cr (cr) USING BTREE,
  KEY click_cr (click_cr) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS rec_themis.goods_id_country_behave(
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_id bigint(20) unsigned NOT NULL COMMENT '商品id',
  country varchar(32) NOT NULL DEFAULT '' COMMENT '国家',
  clicks bigint(20) NOT NULL DEFAULT '0' COMMENT '列表点击',
  impressions bigint(20) NOT NULL DEFAULT '0' COMMENT '列表展示',
  sales_order bigint(20) NOT NULL DEFAULT '0' COMMENT '销量',
  users bigint(20) NOT NULL DEFAULT '0' COMMENT '详情访问',
  impression_users bigint(20) NOT NULL DEFAULT '0' COMMENT '详情访问',
  gmv double NOT NULL DEFAULT '0.00' COMMENT '成交额',
  ctr double NOT NULL DEFAULT '0.0000' COMMENT 'ctr',
  gcr double NOT NULL DEFAULT '0.0000' COMMENT 'gcr',
  cr double NOT NULL DEFAULT '0.0000' COMMENT 'cr',
  click_cr double NOT NULL DEFAULT '0.0000' COMMENT 'click_cr',
  last_update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY unique_key (goods_id,country) USING BTREE,
  KEY country (country)USING BTREE,
  KEY sales_order (sales_order) USING BTREE,
  KEY gmv (gmv) USING BTREE,
  KEY ctr (ctr) USING BTREE,
  KEY gcr (gcr) USING BTREE,
  KEY cr (cr) USING BTREE,
  KEY click_cr (click_cr) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
"

mysql -h rec-themis.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwthemiswriter -pZ1OtPRLkrUIusK0EeYO9Xjha7a79oToz -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi


sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dsqoop.export.records.per.statement=1000 \
-Dmapreduce.map.memory.mb=8192 \
-Dmapreduce.reduce.memory.mb=8192 \
--connect jdbc:mysql://rec-themis.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_themis \
--username dwthemiswriter --password Z1OtPRLkrUIusK0EeYO9Xjha7a79oToz \
--m 1 \
--table goods_id_country_behave_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_goods_id_country_behave \
--hcatalog-partition-keys pt  \
--hcatalog-partition-values  ${pt} \
--fields-terminated-by '\001' \
--columns "goods_id,
country,
clicks,
impressions,
sales_order,
users,
impression_users,
gmv,
ctr,
gcr,
cr,
click_cr"
if [ $? -ne 0 ];then
   exit 1
fi

echo "----------开始rename-------"
mysql -h rec-themis.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwthemiswriter -pZ1OtPRLkrUIusK0EeYO9Xjha7a79oToz <<EOF
rename table rec_themis.goods_id_country_behave to rec_themis.goods_id_country_behave_pre,rec_themis.goods_id_country_behave_new to rec_themis.goods_id_country_behave;
EOF
echo "-------rename结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi