#!/bin/bash
etime=$1
if [ ! -n "$1" ]; then
  etime=`date -d "0 hour" "+%Y-%m-%d %H:00:00"`
fi
echo "etime=$etime"
pt=`date -d "$etime" +%Y-%m-%d`
echo "pt=$pt"
sql="
drop table if exists themis.goods_sn_behave_new;
drop table if exists themis.goods_sn_behave_pre;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS themis.goods_sn_behave_new (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_sn varchar(60) NOT NULL DEFAULT '',
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
  rate double NOT NULL DEFAULT '0' COMMENT 'rate',
  gr double NOT NULL DEFAULT '0' COMMENT 'gr',
  cart_rate double NOT NULL DEFAULT '0' COMMENT 'cart_rate',
  cart_uv bigint(20) NOT NULL DEFAULT '0' COMMENT 'cart_uv',
  cart_pv bigint(20) NOT NULL DEFAULT '0' COMMENT 'cart_pv',
  shop_price double(20,2) NOT NULL DEFAULT '0' COMMENT '商品价格',
  show_price double(20,2) NOT NULL DEFAULT '0' COMMENT '商品价格加运费',
  brand_id bigint(20) NOT NULL DEFAULT '0' COMMENT '品牌id',
  last_update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY goods_sn (goods_sn) USING BTREE,
  KEY sales_order (sales_order) USING BTREE,
  KEY gmv (gmv) USING BTREE,
  KEY ctr (ctr) USING BTREE,
  KEY gcr (gcr) USING BTREE,
  KEY cr (cr) USING BTREE,
  KEY click_cr (click_cr) USING BTREE,
  KEY rate (rate) USING BTREE,
  KEY gr (gr) USING BTREE,
  KEY cart_rate (cart_rate) USING BTREE,
  KEY impressions (impressions) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS themis.goods_sn_behave (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_sn varchar(60) NOT NULL DEFAULT '',
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
  rate double NOT NULL DEFAULT '0' COMMENT 'rate',
  gr double NOT NULL DEFAULT '0' COMMENT 'gr',
  cart_rate double NOT NULL DEFAULT '0' COMMENT 'cart_rate',
  cart_uv bigint(20) NOT NULL DEFAULT '0' COMMENT 'cart_uv',
  cart_pv bigint(20) NOT NULL DEFAULT '0' COMMENT 'cart_pv',
  shop_price double(20,2) NOT NULL DEFAULT '0' COMMENT '商品价格',
  show_price double(20,2) NOT NULL DEFAULT '0' COMMENT '商品价格加运费',
  brand_id bigint(20) NOT NULL DEFAULT '0' COMMENT '品牌id',
  last_update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY goods_sn (goods_sn) USING BTREE,
  KEY sales_order (sales_order) USING BTREE,
  KEY gmv (gmv) USING BTREE,
  KEY ctr (ctr) USING BTREE,
  KEY gcr (gcr) USING BTREE,
  KEY cr (cr) USING BTREE,
  KEY click_cr (click_cr) USING BTREE,
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
--table goods_sn_behave_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_goods_sn_behave \
--hcatalog-partition-keys pt  \
--hcatalog-partition-values  ${pt} \
--fields-terminated-by '\001' \
--columns "goods_sn,
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

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table themis.goods_sn_behave to themis.goods_sn_behave_pre,themis.goods_sn_behave_new to themis.goods_sn_behave;
EOF
echo "-------rename结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
















