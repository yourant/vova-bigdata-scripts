#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "${pre_date}, 检查文件是否生成"
file_num=`aws s3 ls s3://vova-mlb/REC/data/goods_score_data/goods_score/pt=${pre_date}/ | wc -l`
if [ ${file_num} -eq 0 ]; then
  echo "pt=${pre_date} file num = 0"
  exit 1
fi
echo "file_num: ${file_num}"

echo "# 刷新表元数据"
hive -e "msck repair table mlb.mlb_vova_rec_b_goods_score_d;"
if [ $? -ne 0 ];then
  exit 1
fi

echo "# 检查分区数据量"
cnt=$(spark-sql -e "select count(*) from mlb.mlb_vova_rec_b_goods_score_d where pt ='${pre_date}';" |tail -1)
if [ ${cnt} -le 0 ];then
  echo "Error: count(*)=${cnt} -le 0"
  exit 1
fi
echo ${cnt}

# 9455 商品评分表增加分类字段
# 只新增字段，无分区，每天覆盖
echo "# 补充表更新"
job_name="mlb_vova_rec_b_catgoods_score_d_supplement"
sparksql="
insert overwrite table mlb.mlb_vova_rec_b_goods_score_d_supplement
select /*+ REPARTITION(3) */
  gs.goods_id              goods_id        ,
  gs.base_score            base_score      ,
  gs.hot_score             hot_score       ,
  gs.conversion_score      conversion_score,
  gs.honor_score           honor_score     ,
  gs.overall_score         overall_score   ,
  nvl(dg.first_cat_id , 0) first_cat_id ,
  nvl(dg.second_cat_id, 0) second_cat_id,
  nvl(dg.third_cat_id , 0) third_cat_id ,
  nvl(dg.fourth_cat_id, 0) fourth_cat_id
from
  mlb.mlb_vova_rec_b_goods_score_d gs
left join
  dim.dim_vova_goods dg
on gs.goods_id = dg.goods_id
where gs.pt = '${pre_date}'
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sparksql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

echo "# mysql 建表"
sql="
drop table if exists rec_recall.ads_rec_b_goods_score_d_pre;
drop table if exists rec_recall.ads_rec_b_goods_score_d_new;

create table rec_recall.ads_rec_b_goods_score_d_new (
  id                bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  goods_id          int(11)     NOT NULL COMMENT '商品id',
  base_score        DOUBLE      NOT NULL COMMENT '基础评分',
  hot_score         DOUBLE      NOT NULL COMMENT '热度评分',
  conversion_score  DOUBLE      NOT NULL COMMENT '转化评分',
  honor_score       DOUBLE      NOT NULL COMMENT '履约评分',
  overall_score     DOUBLE      NOT NULL COMMENT '综合评分',

  first_cat_id          bigint(11)  DEFAULT 0 NOT NULL COMMENT '商品一级类目ID',
  second_cat_id         bigint(11)  DEFAULT 0 NOT NULL COMMENT '商品二级类目ID',
  third_cat_id          bigint(11)  DEFAULT 0 NOT NULL COMMENT '商品三级类目ID',
  fourth_cat_id         bigint(11)  DEFAULT 0 NOT NULL COMMENT '商品四级类目ID',

  update_time       datetime    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE,
  KEY idx_score_goods (overall_score,goods_id) USING BTREE,
  KEY idx_first_score (first_cat_id,overall_score,goods_id) USING BTREE,
  KEY idx_second_score (second_cat_id,overall_score,goods_id) USING BTREE,
  KEY idx_third_score (third_cat_id,overall_score,goods_id) USING BTREE,
  KEY idx_fourth_score (fourth_cat_id,overall_score,goods_id) USING BTREE
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品综合评分表(搜索及mostpopular)'
;

create table if not exists rec_recall.ads_rec_b_goods_score_d (
  id                bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  goods_id          int(11)     NOT NULL COMMENT '商品id',
  base_score        DOUBLE      NOT NULL COMMENT '基础评分',
  hot_score         DOUBLE      NOT NULL COMMENT '热度评分',
  conversion_score  DOUBLE      NOT NULL COMMENT '转化评分',
  honor_score       DOUBLE      NOT NULL COMMENT '履约评分',
  overall_score     DOUBLE      NOT NULL COMMENT '综合评分',

  first_cat_id          bigint(11)  DEFAULT 0 NOT NULL COMMENT '商品一级类目ID',
  second_cat_id         bigint(11)  DEFAULT 0 NOT NULL COMMENT '商品二级类目ID',
  third_cat_id          bigint(11)  DEFAULT 0 NOT NULL COMMENT '商品三级类目ID',
  fourth_cat_id         bigint(11)  DEFAULT 0 NOT NULL COMMENT '商品四级类目ID',

  update_time       datetime    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE,
  KEY idx_score_goods (overall_score,goods_id) USING BTREE,
  KEY idx_first_score (first_cat_id,overall_score,goods_id) USING BTREE,
  KEY idx_second_score (second_cat_id,overall_score,goods_id) USING BTREE,
  KEY idx_third_score (third_cat_id,overall_score,goods_id) USING BTREE,
  KEY idx_fourth_score (fourth_cat_id,overall_score,goods_id) USING BTREE
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品综合评分表(搜索及mostpopular)'
;
"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

echo "sqoop 导出数据到mysql"
# --hcatalog-partition-keys pt \
# --hcatalog-partition-values ${pre_date} \
sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=10240 \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--m 1 \
--table ads_rec_b_goods_score_d_new \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_rec_b_goods_score_d_supplement \
--columns goods_id,base_score,hot_score,conversion_score,honor_score,overall_score,first_cat_id,second_cat_id,third_cat_id,fourth_cat_id \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
rename table rec_recall.ads_rec_b_goods_score_d to rec_recall.ads_rec_b_goods_score_d_pre,rec_recall.ads_rec_b_goods_score_d_new to rec_recall.ads_rec_b_goods_score_d;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
