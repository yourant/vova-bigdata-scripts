#!/bin/bash

sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_fd_goods_target_7day 60000

if [ $? -ne 0 ];then
  exit 1
fi

sql="
drop table if exists data_report.ads_fd_goods_target_7day_new;
drop table if exists data_report.ads_fd_goods_target_7day_history;

CREATE TABLE IF NOT EXISTS \`data_report\`.\`ads_fd_goods_target_7day_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) COMMENT '商品id',
  \`cat_id\` varchar(50) COMMENT '品类id',
  \`country\` varchar(50) COMMENT '国家',
  \`project\` varchar(50) COMMENT '组织',
  \`platform_type\` varchar(50) COMMENT '平台类型',
  \`impressions\`int(11) COMMENT '预售商品品类列表曝光UV',
  \`click\` int(11) COMMENT '预售商品品类列表点击UV',
  \`users\` int(11) COMMENT '商品详情页UV',
  \`add_session\` int(11) COMMENT '加车UV',
  \`product_add_session\` int(11) COMMENT '详情页加车UV',
  \`orders\` int(11) COMMENT '支付订单数',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`connect_id\` (\`goods_id\`,\`country\`,\`project\`, \`platform_type\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;

CREATE TABLE IF NOT EXISTS \`data_report\`.\`ads_fd_goods_target_7day\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) COMMENT '商品id',
  \`cat_id\` varchar(50) COMMENT '品类id',
  \`country\` varchar(50) COMMENT '国家',
  \`project\` varchar(50) COMMENT '组织',
  \`platform_type\` varchar(50) COMMENT '平台类型',
  \`impressions\`int(11) COMMENT '预售商品品类列表曝光UV',
  \`click\` int(11) COMMENT '预售商品品类列表点击UV',
  \`users\` int(11) COMMENT '商品详情页UV',
  \`add_session\` int(11) COMMENT '加车UV',
  \`product_add_session\` int(11) COMMENT '详情页加车UV',
  \`orders\` int(11) COMMENT '支付订单数',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`connect_id\` (\`goods_id\`,\`country\`,\`project\`, \`platform_type\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
"

ar_host="bd-warehouse-maxscale.gitvv.com"
ar_user="data-report"
ar_pwd='C27PoowhAZIU$LHeI%Gs'
mysql -h "${ar_host}" -u"${ar_user}" -p"${ar_pwd}" -P3313 -N -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://bd-warehouse-maxscale.gitvv.com:3313/data_report \
--username data-report --password 'C27PoowhAZIU$LHeI%Gs' \
--m 1 \
--table ads_fd_goods_target_7day_new \
--hcatalog-database ads \
--hcatalog-table ads_fd_goods_target_7day \
--columns goods_id,cat_id,country,project,platform_type,impressions,click,users,add_session,product_add_session,orders \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h "${ar_host}" -u"${ar_user}" -p"${ar_pwd}" -P3313 <<EOF
rename table data_report.ads_fd_goods_target_7day to data_report.ads_fd_goods_target_7day_history,data_report.ads_fd_goods_target_7day_new to data_report.ads_fd_goods_target_7day;
EOF

echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
