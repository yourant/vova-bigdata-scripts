#!/bin/bash
etime=$1
if [ ! -n "$1" ]; then
  etime=`date -d "0 hour" "+%Y-%m-%d %H:00:00"`
fi
echo "etime=$etime"
pt=`date -d "$etime" +%Y-%m-%d`
echo "pt=$pt"
sql="
drop table if exists rec_themis.goods_id_behave_new;
drop table if exists rec_themis.goods_id_behave_pre;
"
mysql -h rec-themis.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u yzhang13 -pxfwtH3h9sdc2OcKd -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS rec_themis.goods_id_behave_new (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_id bigint(20) NOT NULL DEFAULT '0' comment '商品id',
  goods_sn varchar(60) NOT NULL DEFAULT '',
  clicks bigint(20) NOT NULL DEFAULT '0' comment '列表点击',
  impressions bigint(20) NOT NULL DEFAULT '0' comment '列表展示',
  sales_order bigint(20) NOT NULL DEFAULT '0' comment '销量',
  users bigint(20) NOT NULL DEFAULT '0' comment '点击uv',
  impression_users bigint(20) NOT NULL DEFAULT '0' comment '曝光uv',
  payed_user_num bigint(20) NOT NULL DEFAULT '0' comment '支付uv',
  gmv double NOT NULL DEFAULT '0.00' comment '成交额',
  ctr double NOT NULL DEFAULT '0.0000' comment 'ctr',
  gcr double NOT NULL DEFAULT '0.0000' comment 'gcr',
  sld_gcr double NOT NULL DEFAULT '0.0000' comment 'sld_gcr',
  cr double NOT NULL DEFAULT '0.0000' comment 'cr',
  click_cr double NOT NULL DEFAULT '0.0000' comment 'click_cr',
  grr double NOT NULL DEFAULT '0.0000' comment '非物流退款率',
  sor double NOT NULL DEFAULT '0.0000' comment '7天上网率',
  lgrr double NOT NULL DEFAULT '0.0000' comment '物流退款率',
  comment double NOT NULL DEFAULT '0.0000' comment '评论评分',
  search_score double NOT NULL DEFAULT '0.0000' comment '搜索评分',
  gender int(4) NOT NULL DEFAULT '0' comment '1表示男，2表示女',
  search_click bigint(20) NOT NULL DEFAULT '0' comment '搜索页点击数',
  sales_order_m bigint(20) NOT NULL DEFAULT '0' comment '月销量',
  score double NOT NULL DEFAULT '0' comment '高斯得分',
  rate double NOT NULL DEFAULT '0' comment 'rate',
  gr double NOT NULL DEFAULT '0' comment 'gr',
  cart_rate double NOT NULL DEFAULT '0' comment 'cart_rate',
  cart_uv bigint(20) NOT NULL DEFAULT '0' comment 'cart_uv',
  cart_pv bigint(20) NOT NULL DEFAULT '0' comment 'cart_pv',
  shop_price double NOT NULL DEFAULT '0' comment '商品价格',
  show_price double NOT NULL DEFAULT '0' comment '商品价格加运费',
  brand_id bigint(20) NOT NULL DEFAULT '0' comment '品牌id',
  PRIMARY KEY (id),
  KEY goods_id (goods_id) USING BTREE,
  KEY sales_order (sales_order) USING BTREE,
  KEY goods_sn (goods_sn) USING BTREE,
  KEY gmv (gmv) USING BTREE,
  KEY ctr (ctr) USING BTREE,
  KEY gcr (gcr) USING BTREE,
  KEY sld_gcr (sld_gcr) USING BTREE,
  KEY cr (cr) USING BTREE,
  KEY click_cr (click_cr) USING BTREE,
  KEY grr (grr) USING BTREE,
  KEY sor (sor) USING BTREE,
  KEY lgrr (lgrr) USING BTREE,
  KEY impressions (impressions) USING BTREE,
  KEY gender (gender) USING BTREE,
  KEY search_score (search_score) USING BTREE,
  KEY score (score) USING BTREE,
  KEY comment (comment) USING BTREE,
  KEY search_click (search_click) USING BTREE,
  KEY rate (rate) USING BTREE,
  KEY gr (gr) USING BTREE,
  KEY cart_rate (cart_rate) USING BTREE,
  KEY sales_order_m (sales_order_m) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS rec_themis.goods_id_behave (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_id bigint(20) NOT NULL DEFAULT '0' comment '商品id',
  goods_sn varchar(60) NOT NULL DEFAULT '',
  clicks bigint(20) NOT NULL DEFAULT '0' comment '列表点击',
  impressions bigint(20) NOT NULL DEFAULT '0' comment '列表展示',
  sales_order bigint(20) NOT NULL DEFAULT '0' comment '销量',
  users bigint(20) NOT NULL DEFAULT '0' comment '点击uv',
  impression_users bigint(20) NOT NULL DEFAULT '0' comment '曝光uv',
  payed_user_num bigint(20) NOT NULL DEFAULT '0' comment '支付uv',
  gmv double NOT NULL DEFAULT '0.00' comment '成交额',
  ctr double NOT NULL DEFAULT '0.0000' comment 'ctr',
  gcr double NOT NULL DEFAULT '0.0000' comment 'gcr',
  sld_gcr double NOT NULL DEFAULT '0.0000' comment 'sld_gcr',
  cr double NOT NULL DEFAULT '0.0000' comment 'cr',
  click_cr double NOT NULL DEFAULT '0.0000' comment 'click_cr',
  grr double NOT NULL DEFAULT '0.0000' comment '非物流退款率',
  sor double NOT NULL DEFAULT '0.0000' comment '7天上网率',
  lgrr double NOT NULL DEFAULT '0.0000' comment '物流退款率',
  comment double NOT NULL DEFAULT '0.0000' comment '评论评分',
  search_score double NOT NULL DEFAULT '0.0000' comment '搜索评分',
  gender int(4) NOT NULL DEFAULT '0' comment '1表示男，2表示女',
  search_click bigint(20) NOT NULL DEFAULT '0' comment '搜索页点击数',
  sales_order_m bigint(20) NOT NULL DEFAULT '0' comment '月销量',
  score double NOT NULL DEFAULT '0' comment '高斯得分',
  rate double NOT NULL DEFAULT '0' comment 'rate',
  gr double NOT NULL DEFAULT '0' comment 'gr',
  cart_rate double NOT NULL DEFAULT '0' comment 'cart_rate',
  cart_uv bigint(20) NOT NULL DEFAULT '0' comment 'cart_uv',
  cart_pv bigint(20) NOT NULL DEFAULT '0' comment 'cart_pv',
  shop_price double NOT NULL DEFAULT '0' comment '商品价格',
  show_price double NOT NULL DEFAULT '0' comment '商品价格加运费',
  brand_id bigint(20) NOT NULL DEFAULT '0' comment '品牌id',
  PRIMARY KEY (id),
  KEY goods_id (goods_id) USING BTREE,
  KEY sales_order (sales_order) USING BTREE,
  KEY goods_sn (goods_sn) USING BTREE,
  KEY gmv (gmv) USING BTREE,
  KEY ctr (ctr) USING BTREE,
  KEY gcr (gcr) USING BTREE,
  KEY sld_gcr (sld_gcr) USING BTREE,
  KEY cr (cr) USING BTREE,
  KEY click_cr (click_cr) USING BTREE,
  KEY grr (grr) USING BTREE,
  KEY sor (sor) USING BTREE,
  KEY lgrr (lgrr) USING BTREE,
  KEY impressions (impressions) USING BTREE,
  KEY gender (gender) USING BTREE,
  KEY search_score (search_score) USING BTREE,
  KEY score (score) USING BTREE,
  KEY comment (comment) USING BTREE,
  KEY search_click (search_click) USING BTREE,
  KEY rate (rate) USING BTREE,
  KEY gr (gr) USING BTREE,
  KEY cart_rate (cart_rate) USING BTREE,
  KEY sales_order_m (sales_order_m) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"

mysql -h rec-themis.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u yzhang13 -pxfwtH3h9sdc2OcKd -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi


sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-themis.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_themis \
--username yzhang13 --password xfwtH3h9sdc2OcKd \
--m 1 \
--table goods_id_behave_new \
--hcatalog-database ads \
--hcatalog-table ads_goods_id_behave \
--hcatalog-partition-keys pt  \
--hcatalog-partition-values  ${pt} \
--fields-terminated-by '\001' \
--columns "goods_id,
goods_sn,
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
search_click,
sales_order_m,
gender,
comment,
search_score,
score,
rate,
gr,
cart_uv,
cart_pv,
cart_rate,
shop_price,
show_price,
brand_id,
sld_gcr"
if [ $? -ne 0 ];then
   exit 1
fi

echo "----------开始rename-------"
mysql -h rec-themis.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u yzhang13 -pxfwtH3h9sdc2OcKd <<EOF
rename table rec_themis.goods_id_behave to rec_themis.goods_id_behave_pre,rec_themis.goods_id_behave_new to rec_themis.goods_id_behave;
EOF
echo "-------rename结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi