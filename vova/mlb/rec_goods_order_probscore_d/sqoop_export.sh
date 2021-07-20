#!/bin/bash
#指定日期和引擎
pre_date=$1

#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
echo ${pre_date}

uri="ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com"
dataRow='{
  "data":{
    "jname":"mlb_vova_rec_goods_order_probscore_d",
    "from":"mlb",
    "to":"data",
    "valid_hour":1
  }
}'
# echo "${dataRow}"
resp=`curl ${uri}/vova/api/jobmss/get -s -H "Content-Type:application/json" -X POST --data-raw "${dataRow}"`
echo ${resp}

freedoms=`echo $resp | jq '.data' | jq '.freedoms' | sed -e 's/^"//' -e 's/"$//' | sed 's.\\\\..g'`
echo ${freedoms}
pt=`echo ${freedoms} | jq '.dt' | sed $'s/\"//g'`
echo "pt: ${pt}"

echo "pre_date: ${pre_date}"
#默认日期为昨天
if [ $pt = "null" ]; then
  echo "pt IS NULL"
else
  pre_date=${pt}
fi

echo "pre_date: ${pre_date}"

file_num=`aws s3 ls s3://vova-mlb/REC/data/base/mlb_vova_rec_goods_order_probscore_d/pt=${pre_date}/ | wc -l`
if [ ${file_num} -eq 0 ]; then
  echo "pt=${pre_date} file num = 0"
  exit 1
fi

hive -e "msck repair table mlb.mlb_vova_rec_goods_order_probscore_d;"
if [ $? -ne 0 ];then
  exit 1
fi

cnt=$(spark-sql -e "select count(*) from mlb.mlb_vova_rec_goods_order_probscore_d where pt ='${pre_date}';" |tail -1)
if [ ${cnt} -le 0 ];then
  echo "Error: count(*)=${cnt} -le 0"
  exit 1
fi
echo ${cnt}

sql="
drop table if exists rec_recall.mlb_vova_rec_goods_order_probscore_d_pre;
drop table if exists rec_recall.mlb_vova_rec_goods_order_probscore_d_new;
create table rec_recall.mlb_vova_rec_goods_order_probscore_d_new(
id               int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
goods_id         int(11) NOT NULL  COMMENT '商品id',
first_cat_id     int(11) NOT NULL  COMMENT '一级品类',
second_cat_id    int(11) DEFAULT 0 COMMENT '二级品类',
third_cat_id     int(11) DEFAULT 0 COMMENT '三级品类',
fourth_cat_id    int(11) DEFAULT 0 COMMENT '四级品类',
is_brand         int(11) DEFAULT 0 COMMENT '是否brand',
order_probscore  double  NOT NULL  COMMENT '成交概率分',

PRIMARY KEY (id) USING BTREE,
UNIQUE KEY goods_id (goods_id) USING BTREE,
KEY idx_first_score_goods (first_cat_id, order_probscore, goods_id),
KEY idx_second_score_goods (second_cat_id, order_probscore, goods_id),
KEY idx_third_score_goods (third_cat_id, order_probscore, goods_id),
KEY idx_fourth_score_goods (fourth_cat_id, order_probscore, goods_id),
KEY idx_score_goods_brand (order_probscore, goods_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品成交概率分结果表';

create table if not exists rec_recall.mlb_vova_rec_goods_order_probscore_d (
id               int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
goods_id         int(11) NOT NULL  COMMENT '商品id',
first_cat_id     int(11) NOT NULL  COMMENT '一级品类',
second_cat_id    int(11) DEFAULT 0 COMMENT '二级品类',
third_cat_id     int(11) DEFAULT 0 COMMENT '三级品类',
fourth_cat_id    int(11) DEFAULT 0 COMMENT '四级品类',
is_brand         int(11) DEFAULT 0 COMMENT '是否brand',
order_probscore  double  NOT NULL  COMMENT '成交概率分',

PRIMARY KEY (id) USING BTREE,
UNIQUE KEY goods_id (goods_id) USING BTREE,
KEY idx_first_score_goods (first_cat_id, order_probscore, goods_id),
KEY idx_second_score_goods (second_cat_id, order_probscore, goods_id),
KEY idx_third_score_goods (third_cat_id, order_probscore, goods_id),
KEY idx_fourth_score_goods (fourth_cat_id, order_probscore, goods_id),
KEY idx_score_goods_brand (order_probscore, goods_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品成交概率分结果表';
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
--table mlb_vova_rec_goods_order_probscore_d_new \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_rec_goods_order_probscore_d \
--columns goods_id,first_cat_id,second_cat_id,third_cat_id,fourth_cat_id,is_brand,order_probscore \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
rename table rec_recall.mlb_vova_rec_goods_order_probscore_d to rec_recall.mlb_vova_rec_goods_order_probscore_d_pre,rec_recall.mlb_vova_rec_goods_order_probscore_d_new to rec_recall.mlb_vova_rec_goods_order_probscore_d;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
