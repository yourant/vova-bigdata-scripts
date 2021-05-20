#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pt=$(date -d "-1 day" +%Y-%m-%d)
fi
echo "pt=$pt"
sql="
drop table if exists themis_logistics_report.goods_id_behave_m_new;
drop table if exists themis_logistics_report.goods_id_behave_m_pre;
"
mysql -h db-logistics-w.gitvv.com -u vvreport20210517 -pthuy*at1OhG1eiyoh8she -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS themis_logistics_report.goods_id_behave_m_new (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_id bigint(20) unsigned NOT NULL COMMENT '商品id',
  sales_order bigint(20) NOT NULL DEFAULT '0' COMMENT '销量',
  last_update_time   timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY goods_id (goods_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS themis_logistics_report.goods_id_behave_m (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_id bigint(20) unsigned NOT NULL COMMENT '商品id',
  sales_order bigint(20) NOT NULL DEFAULT '0' COMMENT '销量',
  last_update_time   timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY goods_id (goods_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
"

mysql -h db-logistics-w.gitvv.com -u vvreport20210517 -pthuy*at1OhG1eiyoh8she -e "${sql}"
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
--connect jdbc:mariadb:aurora://db-logistics-w.gitvv.com:3306/themis_logistics_report \
--username vvreport20210517 --password thuy*at1OhG1eiyoh8she --connection-manager org.apache.sqoop.manager.MySQLManager \
--m 10 \
--table goods_id_behave_m_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_goods_id_behave_m \
--hcatalog-partition-keys pt  \
--hcatalog-partition-values  ${pt} \
--fields-terminated-by '\001' \
--columns "goods_id,
sales_order"
if [ $? -ne 0 ];then
   exit 1
fi

echo "----------开始rename-------"
mysql -h db-logistics-w.gitvv.com -u vvreport20210517 -pthuy*at1OhG1eiyoh8she <<EOF
rename table themis_logistics_report.goods_id_behave_m to themis_logistics_report.goods_id_behave_m_pre,themis_logistics_report.goods_id_behave_m_new to themis_logistics_report.goods_id_behave_m;
EOF
echo "-------rename结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


