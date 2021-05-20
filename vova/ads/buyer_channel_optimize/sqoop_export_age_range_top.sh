#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "${pre_date}"

sql="
drop table if exists rec_recall.ads_vova_age_range_top_pre;
drop table if exists rec_recall.ads_vova_age_range_top_new;
create table rec_recall.ads_vova_age_range_top_new(
  id          int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  age_range   varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '所属年龄段',
  goods_id    int(11) NOT NULL COMMENT '商品id',
  create_time timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (id) USING BTREE
  ) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '统计0~18和18~35年龄段用户，各取两个段内销量最高的top100商品' ROW_FORMAT = Dynamic;

create table if not exists rec_recall.ads_vova_age_range_top (
  id          int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  age_range   varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '所属年龄段',
  goods_id    int(11) NOT NULL COMMENT '商品id',
  create_time timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (id) USING BTREE
  ) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '统计0~18和18~35年龄段用户，各取两个段内销量最高的top100商品' ROW_FORMAT = Dynamic;
"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8096 \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--m 1 \
--table ads_vova_age_range_top_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_age_range_top \
--columns age_range,goods_id \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
rename table rec_recall.ads_vova_age_range_top to rec_recall.ads_vova_age_range_top_pre,rec_recall.ads_vova_age_range_top_new to rec_recall.ads_vova_age_range_top;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
