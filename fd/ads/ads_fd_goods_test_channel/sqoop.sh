#!/bin/bash

sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_fd_goods_test_channel 1

if [ $? -ne 0 ];then
  exit 1
fi

sql="
drop table if exists data_report.ads_fd_goods_test_channel_new;
drop table if exists data_report.ads_fd_goods_test_channel_history;

CREATE TABLE IF NOT EXISTS \`data_report\`.\`ads_fd_goods_test_channel_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`cat_name\` varchar(50) COMMENT '品类名',
  \`selection_channel\` varchar(50) COMMENT '选款渠道',
  \`channel_type\` varchar(50) COMMENT '渠道类型',
  \`number_of_success_products\` int(11) COMMENT '成功商品数',
  \`number_of_end_products\` int(11) COMMENT '结束商品数',
  \`success_rate\` decimal(15,4) COMMENT '成功率',
  \`last_7_days_goods_sales\` decimal(15,4) COMMENT '成功商品近7天销售额',
  \`channel_contribution\` decimal(15,4) COMMENT '渠道贡献度',
  \`number_of_popular_products\` int(11) COMMENT '爆款商品数',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;

CREATE TABLE IF NOT EXISTS \`data_report\`.\`ads_fd_goods_test_channel\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`cat_name\` varchar(50) COMMENT '品类名',
  \`selection_channel\` varchar(50) COMMENT '选款渠道',
  \`channel_type\` varchar(50) COMMENT '渠道类型',
  \`number_of_success_products\` int(11) COMMENT '成功商品数',
  \`number_of_end_products\` int(11) COMMENT '结束商品数',
  \`success_rate\` decimal(15,4) COMMENT '成功率',
  \`last_7_days_goods_sales\` decimal(15,4) COMMENT '成功商品近7天销售额',
  \`channel_contribution\` decimal(15,4) COMMENT '渠道贡献度',
  \`number_of_popular_products\` int(11) COMMENT '爆款商品数',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE
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
--table ads_fd_goods_test_channel_new \
--hcatalog-database ads \
--hcatalog-table ads_fd_goods_test_channel \
--columns cat_name,selection_channel,channel_type,number_of_success_products,number_of_end_products,success_rate,last_7_days_goods_sales,channel_contribution,number_of_popular_products \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h "${ar_host}" -u"${ar_user}" -p"${ar_pwd}" -P3313 <<EOF
rename table data_report.ads_fd_goods_test_channel to data_report.ads_fd_goods_test_channel_history,data_report.ads_fd_goods_test_channel_new to data_report.ads_fd_goods_test_channel;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi