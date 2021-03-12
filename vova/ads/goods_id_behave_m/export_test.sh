#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pt=$(date -d "-1 day" +%Y-%m-%d)
fi
echo "pt=$pt"
sql="
drop table if exists themis.goods_id_behave_m_new;
drop table if exists themis.goods_id_behave_m_pre;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS themis.goods_id_behave_m_new (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_id bigint(20) unsigned NOT NULL COMMENT '商品id',
  clicks bigint(20) NOT NULL DEFAULT '0' COMMENT '列表点击',
  impressions bigint(20) NOT NULL DEFAULT '0' COMMENT '列表展示',
  sales_order bigint(20) NOT NULL DEFAULT '0' COMMENT '销量',
  users bigint(20) NOT NULL DEFAULT '0' COMMENT '点击uv',
  impression_users bigint(20) NOT NULL DEFAULT '0' COMMENT '曝光uv',
  payed_user_num bigint(20) NOT NULL DEFAULT '0' COMMENT '支付uv',
  gmv double NOT NULL DEFAULT '0.00' COMMENT '成交额',
  ctr double NOT NULL DEFAULT '0.0000' COMMENT 'ctr',
  gcr double NOT NULL DEFAULT '0.0000' COMMENT 'gcr',
  cr double NOT NULL DEFAULT '0.0000' COMMENT 'cr',
  click_cr double NOT NULL DEFAULT '0.0000' COMMENT 'click_cr',
  grr double NOT NULL DEFAULT '0.0000' COMMENT '非物流退款率',
  sor double NOT NULL DEFAULT '0.0000' COMMENT '7天上网率',
  lgrr double NOT NULL DEFAULT '0.0000' COMMENT '物流退款率',
  rate double NOT NULL DEFAULT '0' COMMENT 'rate',
  gr double NOT NULL DEFAULT '0' COMMENT 'gr',
  cart_rate double NOT NULL DEFAULT '0' COMMENT 'cart_rate',
  cart_uv bigint(20) NOT NULL DEFAULT '0' COMMENT 'cart_uv',
  cart_pv bigint(20) NOT NULL DEFAULT '0' COMMENT 'cart_pv',
  shop_price double NOT NULL DEFAULT '0' COMMENT '商品价格',
  show_price double NOT NULL DEFAULT '0' COMMENT '商品价格加运费',
  brand_id bigint(20) NOT NULL DEFAULT '0' COMMENT '品牌id',
  last_update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY unique_key (goods_id) USING BTREE,
  KEY sales_order (sales_order) USING BTREE,
  KEY gmv (gmv) USING BTREE,
  KEY ctr (ctr) USING BTREE,
  KEY gcr (gcr) USING BTREE,
  KEY cr (cr) USING BTREE,
  KEY click_cr (click_cr) USING BTREE,
  KEY grr (grr) USING BTREE,
  KEY sor (sor) USING BTREE,
  KEY lgrr (lgrr) USING BTREE,
  KEY rate (rate) USING BTREE,
  KEY gr (gr) USING BTREE,
  KEY cart_rate (cart_rate) USING BTREE,
  KEY impressions (impressions) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS themis.goods_id_behave_m (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_id bigint(20) unsigned NOT NULL COMMENT '商品id',
  clicks bigint(20) NOT NULL DEFAULT '0' COMMENT '列表点击',
  impressions bigint(20) NOT NULL DEFAULT '0' COMMENT '列表展示',
  sales_order bigint(20) NOT NULL DEFAULT '0' COMMENT '销量',
  users bigint(20) NOT NULL DEFAULT '0' COMMENT '点击uv',
  impression_users bigint(20) NOT NULL DEFAULT '0' COMMENT '曝光uv',
  payed_user_num bigint(20) NOT NULL DEFAULT '0' COMMENT '支付uv',
  gmv double NOT NULL DEFAULT '0.00' COMMENT '成交额',
  ctr double NOT NULL DEFAULT '0.0000' COMMENT 'ctr',
  gcr double NOT NULL DEFAULT '0.0000' COMMENT 'gcr',
  cr double NOT NULL DEFAULT '0.0000' COMMENT 'cr',
  click_cr double NOT NULL DEFAULT '0.0000' COMMENT 'click_cr',
  grr double NOT NULL DEFAULT '0.0000' COMMENT '非物流退款率',
  sor double NOT NULL DEFAULT '0.0000' COMMENT '7天上网率',
  lgrr double NOT NULL DEFAULT '0.0000' COMMENT '物流退款率',
  rate double NOT NULL DEFAULT '0' COMMENT 'rate',
  gr double NOT NULL DEFAULT '0' COMMENT 'gr',
  cart_rate double NOT NULL DEFAULT '0' COMMENT 'cart_rate',
  cart_uv bigint(20) NOT NULL DEFAULT '0' COMMENT 'cart_uv',
  cart_pv bigint(20) NOT NULL DEFAULT '0' COMMENT 'cart_pv',
  shop_price double NOT NULL DEFAULT '0' COMMENT '商品价格',
  show_price double NOT NULL DEFAULT '0' COMMENT '商品价格加运费',
  brand_id bigint(20) NOT NULL DEFAULT '0' COMMENT '品牌id',
  last_update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY unique_key (goods_id) USING BTREE,
  KEY sales_order (sales_order) USING BTREE,
  KEY gmv (gmv) USING BTREE,
  KEY ctr (ctr) USING BTREE,
  KEY gcr (gcr) USING BTREE,
  KEY cr (cr) USING BTREE,
  KEY click_cr (click_cr) USING BTREE,
  KEY grr (grr) USING BTREE,
  KEY sor (sor) USING BTREE,
  KEY lgrr (lgrr) USING BTREE,
  KEY rate (rate) USING BTREE,
  KEY gr (gr) USING BTREE,
  KEY cart_rate (cart_rate) USING BTREE,
  KEY impressions (impressions) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--m 1 \
--table goods_id_behave_m_new \
--hcatalog-database ads \
--hcatalog-table ads_goods_id_behave_m \
--hcatalog-partition-keys pt  \
--hcatalog-partition-values  ${pt} \
--fields-terminated-by '\001' \
--columns "goods_id,
clicks,
impressions,
sales_order,
users,
impression_users,
payed_user_num,
gmv,
ctr,
gcr,
cr,
click_cr,
grr,
sor,
lgrr,
rate,
gr,
cart_uv,
cart_pv,
cart_rate,
shop_price,
show_price,
brand_id"
if [ $? -ne 0 ];then
   exit 1
fi






