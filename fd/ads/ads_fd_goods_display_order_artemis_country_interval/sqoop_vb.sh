#!/bin/bash

sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_fd_goods_display_order_artemis_country_interval 10000

if [ $? -ne 0 ];then
  exit 1
fi

ts=$(date +"%Y-%m-%d %H")
if [ "$#" -ne 1 ]; then
  pt=$(date -d "$ts -40 hours" +"%Y%m%d")
else
  pt=$1
fi

echo "pt: ${pt}"

sql="
CREATE TABLE IF NOT EXISTS \`vbridal\`.\`goods_display_order_artemis_country_interval_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) COMMENT '商品ID',
  \`country_code\` varchar(50) COMMENT '国家',
  \`project_name\` varchar(50) COMMENT '组织',
  \`platform\` varchar(50) COMMENT '平台',
  \`impressions\` int(11) COMMENT '预售商品品类列表曝光UV',
  \`clicks\` int(11) COMMENT '预售商品品类列表点击UV',
  \`users\` int(11) COMMENT '商品详情页UV',
  \`sales_order\` int(11) COMMENT '销量排序',
  \`detail_add_cart\` int(11) COMMENT '详情页加车UV',
  \`list_add_cart\` int(11) COMMENT '品类列表加车UV',
  \`checkout\` int(11) COMMENT '支付订单数',
  \`sales_order_in_7_days\` int(11) COMMENT '支付订单数',
  \`virtual_sales_order\` int(11) COMMENT '默认0',
  \`goods_order\` int(11) COMMENT '默认0',
  \`start_time\` datetime COMMENT '开始时间',
  \`end_time\` datetime COMMENT '结束时间',
  \`interval\` varchar(50) COMMENT '标记',
  \`is_active\` int(11) COMMENT '默认1',
  \`sales\` int(11) COMMENT '商品销量（即销售件数）',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE KEY \`goods_id\` (\`goods_id\`,\`country_code\`,\`project_name\`,\`platform\`,\`end_time\`,\`interval\`) USING BTREE,
  KEY \`sales_order\` (\`sales_order\`) USING BTREE,
  KEY \`country_code\` (\`country_code\`),
  KEY \`start_time\` (\`start_time\`),
  KEY \`end_time\` (\`end_time\`),
  KEY \`project_name_platform\` (\`project_name\`,\`platform\`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;

CREATE TABLE IF NOT EXISTS \`vbridal\`.\`goods_display_order_artemis_country_interval\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) COMMENT '商品ID',
  \`country_code\` varchar(50) COMMENT '国家',
  \`project_name\` varchar(50) COMMENT '组织',
  \`platform\` varchar(50) COMMENT '平台',
  \`impressions\` int(11) COMMENT '预售商品品类列表曝光UV',
  \`clicks\` int(11) COMMENT '预售商品品类列表点击UV',
  \`users\` int(11) COMMENT '商品详情页UV',
  \`sales_order\` int(11) COMMENT '销量排序',
  \`detail_add_cart\` int(11) COMMENT '详情页加车UV',
  \`list_add_cart\` int(11) COMMENT '品类列表加车UV',
  \`checkout\` int(11) COMMENT '支付订单数',
  \`sales_order_in_7_days\` int(11) COMMENT '支付订单数',
  \`virtual_sales_order\` int(11) COMMENT '默认0',
  \`goods_order\` int(11) COMMENT '默认0',
  \`start_time\` datetime COMMENT '开始时间',
  \`end_time\` datetime COMMENT '结束时间',
  \`interval\` varchar(50) COMMENT '标记',
  \`is_active\` int(11) COMMENT '默认1',
  \`sales\` int(11) COMMENT '商品销量（即销售件数）',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE KEY \`goods_id\` (\`goods_id\`,\`country_code\`,\`project_name\`,\`platform\`,\`end_time\`,\`interval\`) USING BTREE,
  KEY \`sales_order\` (\`sales_order\`) USING BTREE,
  KEY \`country_code\` (\`country_code\`),
  KEY \`start_time\` (\`start_time\`),
  KEY \`end_time\` (\`end_time\`),
  KEY \`project_name_platform\` (\`project_name\`,\`platform\`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
"

ar_host="bd-warehouse-maxscale.gitvv.com"
ar_user="artemisnew"
ar_pwd="apqBtPe8TJA407Kpp9by"

mysql -h "${ar_host}" -u"${ar_user}" -p"${ar_pwd}" -P3309 -N -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://bd-warehouse-maxscale.gitvv.com:3309/vbridal \
--username artemisnew --password 'apqBtPe8TJA407Kpp9by' \
--m 1 \
--table goods_display_order_artemis_country_interval_new \
--hcatalog-database ads \
--hcatalog-table ads_fd_goods_display_order_artemis_country_interval \
--columns goods_id,country_code,project_name,platform,impressions,clicks,users,sales_order,detail_add_cart,list_add_cart,checkout,sales_order_in_7_days,virtual_sales_order,goods_order,start_time,end_time,interval,is_active,sales  \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h "${ar_host}" -u"${ar_user}" -p"${ar_pwd}" -P3309 <<EOF
rename table vbridal.goods_display_order_artemis_country_interval to vbridal.goods_display_order_artemis_country_interval_${pt},vbridal.goods_display_order_artemis_country_interval_new to vbridal.goods_display_order_artemis_country_interval;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
