#!/bin/bash

sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_fd_order_hit 1

if [ $? -ne 0 ];then
  exit 1
fi

sql="
drop table if exists data_report.ads_fd_order_hit_new;
drop table if exists data_report.ads_fd_order_hit_history;

CREATE TABLE IF NOT EXISTS \`data_report\`.\`ads_fd_order_hit_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`timestamp\` timestamp COMMENT '事件时间',
  \`goods_id\` int(255) COMMENT '商品ID',
  \`domain_userid\` varchar(100) COMMENT 'domain 用户ID',
  \`session_id\` varchar(255) COMMENT 'sessionID',
  \`mkt_source\` varchar(255) COMMENT '',
  \`mkt_campaign\` varchar(255) COMMENT '',
  \`mkt_term\` varchar(255) COMMENT '',
  \`mkt_content\` varchar(255) COMMENT '',
  \`mkt_medium\` varchar(255) COMMENT '',
  \`mkt_click_id\` varchar(255) COMMENT '',
  \`mkt_network\` varchar(255) COMMENT '',
  \`page_code\` varchar(255) COMMENT '页面标识',
  \`country\` varchar(255) COMMENT '国家',
  \`language\` varchar(255) COMMENT '语言',
  \`platform\` varchar(255) COMMENT '平台',
  \`os\` varchar(255) COMMENT '系统名字',
  \`os_family\` varchar(255) COMMENT '操作系统',
  \`os_version\` varchar(255) COMMENT '系统版本',
  \`device_type\` varchar(255) COMMENT '设备类型',
  \`geo_country\` varchar(255) COMMENT 'geo国家',
  \`url\` varchar(2000) COMMENT 'URL链接',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;

CREATE TABLE IF NOT EXISTS \`data_report\`.\`ads_fd_order_hit\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`timestamp\` timestamp COMMENT '事件时间',
  \`goods_id\` int(255) COMMENT '商品ID',
  \`domain_userid\` varchar(100) COMMENT 'domain 用户ID',
  \`session_id\` varchar(255) COMMENT 'sessionID',
  \`mkt_source\` varchar(255) COMMENT '',
  \`mkt_campaign\` varchar(255) COMMENT '',
  \`mkt_term\` varchar(255) COMMENT '',
  \`mkt_content\` varchar(255) COMMENT '',
  \`mkt_medium\` varchar(255) COMMENT '',
  \`mkt_click_id\` varchar(255) COMMENT '',
  \`mkt_network\` varchar(255) COMMENT '',
  \`page_code\` varchar(255) COMMENT '页面标识',
  \`country\` varchar(255) COMMENT '国家',
  \`language\` varchar(255) COMMENT '语言',
  \`platform\` varchar(255) COMMENT '平台',
  \`os\` varchar(255) COMMENT '系统名字',
  \`os_family\` varchar(255) COMMENT '操作系统',
  \`os_version\` varchar(255) COMMENT '系统版本',
  \`device_type\` varchar(255) COMMENT '设备类型',
  \`geo_country\` varchar(255) COMMENT 'geo国家',
  \`url\` varchar(2000) COMMENT 'URL链接',
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
--table ads_fd_order_hit_new \
--hcatalog-database ads \
--hcatalog-table ads_fd_order_hit \
--columns domain_userid,session_id,mkt_source,mkt_campaign,mkt_term,mkt_content,mkt_medium,mkt_click_id,mkt_network,page_code,country,language,platform,os,os_family,os_version,device_type,geo_country,url \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h "${ar_host}" -u"${ar_user}" -p"${ar_pwd}" -P3313 <<EOF
rename table data_report.ads_fd_order_hit to data_report.ads_fd_order_hit_history,data_report.ads_fd_order_hit_new to data_report.ads_fd_order_hit;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
