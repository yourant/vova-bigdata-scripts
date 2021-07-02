#!/bin/bash
#指定日期和引擎
pre_date=$1
pre_hour=$2
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date +%Y-%m-%d)
  pre_hour=$(date -d "-1 hour" +%H)

  if [ ${pre_hour} -eq 23 ]; then
      pre_date=$(date -d "-1 day" +%Y-%m-%d)
  fi
fi

echo "pre_date: ${pre_date}"
echo "pre_hour: ${pre_hour}"

file_num=`aws s3 ls s3://vova-mlb/REC/data/match/match_result/mlb_vova_rec_u2i_match_list_h/pt=${pre_date}/hour=${pre_hour}/ | wc -l`
if [ ${file_num} -eq 0 ]; then
  echo "pt=${pre_date}, hour=${pre_hour} file num = 0"
  exit 1
fi

echo "pt=${pre_date}, hour=${pre_hour} file num: ${file_num}"

hive -e "msck repair table mlb.mlb_vova_rec_u2i_match_list_h;"
if [ $? -ne 0 ];then
  exit 1
fi

cnt=$(spark-sql -e "select count(*) from mlb.mlb_vova_rec_u2i_match_list_h where pt ='${pre_date}' and hour='${pre_hour}';" |tail -1)
if [ ${cnt} -le 0 ];then
  echo "Error: count(*)=${cnt} -le 0"
  exit 1
fi
echo "pt ='${pre_date}' and hour='${pre_hour}' cnt: ${cnt}"

# mysql 建表
sql="
create table if not exists rec_recall.mlb_vova_rec_u2i_match_list_h (
  id                    bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  buyer_id              bigint(11)  NOT NULL COMMENT '用户id',
  cat_id                bigint(11)  NOT NULL COMMENT '品类id',
  rec_goods_list        text        NOT NULL COMMENT '推荐商品序列化结果',
  score_list            text        NOT NULL COMMENT '推荐结果得分序列化结果',
  update_time           datetime    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY buyer_cat_id (buyer_id, cat_id) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品一级品类综合评分表(搜索及mostpopular)'
;
"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8096 \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--m 1 \
--table mlb_vova_rec_u2i_match_list_h \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_rec_u2i_match_list_h \
--columns buyer_id,cat_id,rec_goods_list,score_list \
--hcatalog-partition-keys pt,hour \
--hcatalog-partition-values ${pre_date},${pre_hour} \
--update-key buyer_id,cat_id \
--update-mode allowinsert \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi
