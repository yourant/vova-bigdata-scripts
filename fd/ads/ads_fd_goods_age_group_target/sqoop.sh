#!/bin/bash

sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_fd_goods_age_group_target 60000

if [ $? -ne 0 ];then
  exit 1
fi

sql="
drop table if exists data_report.ads_fd_goods_age_group_target_new;
drop table if exists data_report.ads_fd_goods_age_group_target_history;
CREATE TABLE IF NOT EXISTS \`data_report\`.\`ads_fd_goods_age_group_target_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) COMMENT '商品id',
  \`project\` varchar(50) COMMENT '组织',
  \`country\` varchar(50) COMMENT '国家',
  \`platform_type\` varchar(50) COMMENT '设备平台',
  \`age_group\` varchar(50) COMMENT '年龄分层',
  \`clicks\` int(11) COMMENT '品类列表页点击uv',
  \`impressions\` int(11) COMMENT '品类列表页曝光uv',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`goods_id\` (\`goods_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
CREATE TABLE IF NOT EXISTS \`data_report\`.\`ads_fd_goods_age_group_target\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) COMMENT '商品id',
  \`project\` varchar(50) COMMENT '组织',
  \`country\` varchar(50) COMMENT '国家',
  \`platform_type\` varchar(50) COMMENT '设备平台',
  \`age_group\` varchar(50) COMMENT '年龄分层',
  \`clicks\` int(11) COMMENT '品类列表页点击uv',
  \`impressions\` int(11) COMMENT '品类列表页曝光uv',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`goods_id\` (\`goods_id\`)
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
--table ads_fd_goods_age_group_target_new \
--hcatalog-database ads \
--hcatalog-table ads_fd_goods_age_group_target \
--columns goods_id,project,country,platform_type,age_group,clicks,impressions \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h "${ar_host}" -u"${ar_user}" -p"${ar_pwd}" -P3313 <<EOF
rename table data_report.ads_fd_goods_age_group_target to data_report.ads_fd_goods_age_group_target_history,data_report.ads_fd_goods_age_group_target_new to data_report.ads_fd_goods_age_group_target;

EOF

echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi