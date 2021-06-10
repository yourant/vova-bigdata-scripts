#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "检查分区数据量"
cnt=$(spark-sql -e "select count(*) from mlb.mlb_vova_gender_hot_goods_d where pt ='${pre_date}';" |tail -1)
if [ ${cnt} -le 0 ];then
  echo "Error: count(*)=${cnt} -le 0"
  exit 1
fi
echo ${cnt}

# mysql 建表
sql="
drop table if exists rec_recall.mlb_vova_gender_hot_goods_d_pre;
drop table if exists rec_recall.mlb_vova_gender_hot_goods_d_new;

create table if not exists rec_recall.mlb_vova_gender_hot_goods_d_new (
id                   bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '自增主键',
gender               int         NOT NULL COMMENT '性别:1:男；2:女；0:通用',
goods_id             bigint      NOT NULL COMMENT '商品id',
goods_score          double      NOT NULL COMMENT '商品综合评分',
update_time          datetime    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id) USING BTREE,
  KEY gender_goods (gender,goods_id) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='性别热门兜底'
;

create table if not exists rec_recall.mlb_vova_gender_hot_goods_d (
id                   bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '自增主键',
gender               int         NOT NULL COMMENT '性别:1:男；2:女；0:通用',
goods_id             bigint      NOT NULL COMMENT '商品id',
goods_score          double      NOT NULL COMMENT '商品综合评分',
update_time          datetime    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id) USING BTREE,
  KEY gender_goods (gender,goods_id) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='性别热门兜底'
;
"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

echo "# sqoop 导出到 mysql"
sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8096 \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--m 1 \
--table mlb_vova_gender_hot_goods_d_new \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_gender_hot_goods_d \
--columns gender,goods_id,goods_score \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

# rename
echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
rename table rec_recall.mlb_vova_gender_hot_goods_d to rec_recall.mlb_vova_gender_hot_goods_d_pre,rec_recall.mlb_vova_gender_hot_goods_d_new to rec_recall.mlb_vova_gender_hot_goods_d;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
