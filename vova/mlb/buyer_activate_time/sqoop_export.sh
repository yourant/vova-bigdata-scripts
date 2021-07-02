#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "${pre_date}"

sql="
drop table if exists rec_recall.mlb_vova_buyer_activate_time_pre;
drop table if exists rec_recall.mlb_vova_buyer_activate_time_new;
create table rec_recall.mlb_vova_buyer_activate_time_new(
id             int(11)     NOT NULL AUTO_INCREMENT COMMENT '自增主键',
buyer_id       bigint      NOT NULL COMMENT '用户id',
activate_time  varchar(20) NOT NULL COMMENT '激活时间',
PRIMARY KEY (id) USING BTREE,
UNIQUE KEY buyer_id (buyer_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='近180天有pv的用户及激活时间';

create table if not exists rec_recall.mlb_vova_buyer_activate_time (
id             int(11)     NOT NULL AUTO_INCREMENT COMMENT '自增主键',
buyer_id       bigint      NOT NULL COMMENT '用户id',
activate_time  varchar(20) NOT NULL COMMENT '激活时间',
PRIMARY KEY (id) USING BTREE,
UNIQUE KEY buyer_id (buyer_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='近180天有pv的用户及激活时间';
"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8096 \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall?disableMariaDbDriver \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--m 1 \
--table mlb_vova_buyer_activate_time_new \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_buyer_activate_time_day180 \
--columns buyer_id,activate_time \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
rename table rec_recall.mlb_vova_buyer_activate_time to rec_recall.mlb_vova_buyer_activate_time_pre,rec_recall.mlb_vova_buyer_activate_time_new to rec_recall.mlb_vova_buyer_activate_time;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
