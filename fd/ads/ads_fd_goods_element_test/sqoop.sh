#!/bin/bash

sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_fd_goods_element_test 10000

if [ $? -ne 0 ];then
  exit 1
fi

ts=$(date +"%Y-%m-%d %H")
if [ "$#" -ne 1 ]; then
  pt=$(date -d "$ts -16 hours" +"%Y%m%d")
else
  pt=$1
fi

echo "pt: ${pt}"

sql="
drop table if exists data_report.ads_fd_goods_element_test_new;
drop table if exists data_report.ads_fd_goods_element_test_${pt};

CREATE TABLE IF NOT EXISTS \`data_report\`.\`ads_fd_goods_element_test_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) COMMENT '商品ID',
  \`project\` varchar(50) COMMENT '组织',
  \`platform\` varchar(50) COMMENT '平台',
  \`country\` varchar(50) COMMENT '国家',
  \`element_tag\` varchar(50) COMMENT '图片组',
  \`element_batch\` varchar(50) COMMENT '图片批次',
  \`session_common_impression\` int(11) COMMENT '曝光UV',
  \`session_common_click\` int(11) COMMENT '点击UV',
  \`session_common_ctr\` decimal(38, 2) COMMENT '',
  \`views\` int(11) COMMENT '点击UV',
  \`cart\` int(11) COMMENT '点击UV',
  \`video_impression\` int(11) COMMENT '点击UV',
  \`video_play\` int(11) COMMENT '点击UV',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`goods_id\` (\`goods_id\`),
  KEY \`country\` (\`country\`),
  KEY \`project_platform\` (\`project\`,\`platform\`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;

CREATE TABLE IF NOT EXISTS \`data_report\`.\`ads_fd_goods_element_test\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) COMMENT '商品ID',
  \`project\` varchar(50) COMMENT '组织',
  \`platform\` varchar(50) COMMENT '平台',
  \`country\` varchar(50) COMMENT '国家',
  \`element_tag\` varchar(50) COMMENT '图片组',
  \`element_batch\` varchar(50) COMMENT '图片批次',
  \`session_common_impression\` int(11) COMMENT '曝光UV',
  \`session_common_click\` int(11) COMMENT '点击UV',
  \`session_common_ctr\` decimal(38, 2) COMMENT '',
  \`views\` int(11) COMMENT '点击UV',
  \`cart\` int(11) COMMENT '点击UV',
  \`video_impression\` int(11) COMMENT '点击UV',
  \`video_play\` int(11) COMMENT '点击UV',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`goods_id\` (\`goods_id\`),
  KEY \`country\` (\`country\`),
  KEY \`project_platform\` (\`project\`,\`platform\`) USING BTREE
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
--table ads_fd_goods_element_test_new \
--hcatalog-database ads \
--hcatalog-table ads_fd_goods_element_test \
--columns goods_id,project,platform,country,picture_group,picture_batch,impression_session,click_session,session_ctr\
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h "${ar_host}" -u"${ar_user}" -p"${ar_pwd}" -P3313 <<EOF
rename table data_report.ads_fd_goods_element_test to data_report.ads_fd_goods_element_test_${pt},data_report.ads_fd_goods_element_test_new to data_report.ads_fd_goods_element_test;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
