#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
          cur_date=$(date -d "-1 day" +%Y-%m-%d)
fi

mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "
DROP TABLE IF EXISTS rec_recall.ads_vova_rec_m_user_kg_tag_d_bak;
CREATE TABLE IF NOT EXISTS rec_recall.ads_vova_rec_m_user_kg_tag_d_bak (
id int(11) NOT NULL AUTO_INCREMENT,
buyer_id int(11) NOT NULL COMMENT '用户id',
kg_tag_combine_list mediumtext NOT NULL COMMENT '标签列表',
bod_id_list mediumtext NOT NULL COMMENT '榜单id列表',
score_list mediumtext NOT NULL COMMENT '分数列表',
update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (id) USING BTREE,
KEY buyer_id (buyer_id)
) ENGINE=InnoDB AUTO_INCREMENT=1488901 DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱-用户榜单偏好';"

if [ $? -ne 0 ];then
exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=1000 \
-Dmapreduce.map.memory.mb=8192 \
-Dmapreduce.reduce.memory.mb=8192 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--m 1 \
--table ads_vova_rec_m_user_kg_tag_d_bak \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_rec_m_user_kg_tag_d \
--columns buyer_id,kg_tag_combine_list,score_list,bod_id_list \
--hive-partition-key pt \
--hive-partition-value ${cur_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
exit 1
fi
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "
RENAME table rec_recall.ads_vova_rec_m_user_kg_tag_d to rec_recall.ads_vova_rec_m_user_kg_tag_d_tmp;
RENAME table rec_recall.ads_vova_rec_m_user_kg_tag_d_bak to rec_recall.ads_vova_rec_m_user_kg_tag_d;
RENAME table rec_recall.ads_vova_rec_m_user_kg_tag_d_tmp to rec_recall.ads_vova_rec_m_user_kg_tag_d_bak;
"

if [ $? -ne 0 ];then
exit 1
fi
reg='\\|'
sql="insert overwrite table ads.ads_vova_bod_goods_rank_data partition (pt = '${cur_date}')
SELECT bod_id,
bod_name,
goods_id,
rank
from (
SELECT
t3.bod_id as bod_id,
t1.bod_name,t2.goods_id,rank() over(partition by t1.bod_name order by t2.overall_score desc) as rank
from (
SELECT
DISTINCT bod_name,split(bod_name,'${reg}')[0] as v1,split(bod_name,'${reg}')[1] as v2,split(bod_name,'${reg}')[2] as second_cat_id FROM mlb.mlb_vova_rec_m_user_kg_tag_d
lateral view explode(split(kg_tag_combine_list,',')) t as bod_name
where pt='${cur_date}'
) t1 LEFT join
(
select
a.goods_id,
b.v_str,
b.second_cat_id,
a.overall_score
from
mlb.mlb_vova_rec_b_goods_score_d a
LEFT JOIN (
SELECT goods_id,second_cat_id,concat_ws('|',collect_set(attr_value)) as v_str from ads.ads_vova_goods_attribute_label_data
GROUP by goods_id,second_cat_id
) b on a.goods_id=b.goods_id
where a.pt='${cur_date}' and a.overall_score>27 and b.v_str is not null
) t2 on t1.second_cat_id=t2.second_cat_id
left join dim.dim_vova_bod t3 on t1.bod_name=t3.bod_name
where t2.v_str like concat('%',t1.v1,'%') and t2.v_str like concat('%',t1.v2,'%')
) T WHERE rank<=500"
spark-sql --conf "spark.app.name=ads_vova_bod_goods_rank_data" --conf "spark.dynamicAllocation.maxExecutors=150" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
exit 1
fi

mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "
DROP TABLE IF EXISTS rec_recall.ads_vova_bod_goods_rank_data_bak;
CREATE TABLE IF NOT EXISTS rec_recall.ads_vova_bod_goods_rank_data_bak (
id int(11) NOT NULL AUTO_INCREMENT,
bod_id int(11) NOT NULL COMMENT '榜单id',
bod_name varchar(255) NOT NULL COMMENT '榜单名称',
goods_id int(11) NOT NULL COMMENT '商品id',
rank int(11) NOT NULL COMMENT '商品评分排名',
update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (id) USING BTREE,
KEY bod_id (bod_id),
KEY goods_id (goods_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱-榜单商品评分排名统计';
"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
exit 1
fi
sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=1000 \
-Dmapreduce.map.memory.mb=8192 \
-Dmapreduce.reduce.memory.mb=8192 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--m 1 \
--table ads_vova_bod_goods_rank_data_bak \
--hcatalog-database ads \
--hcatalog-table ads_vova_bod_goods_rank_data \
--hive-partition-key pt \
--hive-partition-value ${cur_date} \
--columns bod_id,bod_name,goods_id,rank \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
exit 1
fi

mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "
RENAME table rec_recall.ads_vova_bod_goods_rank_data to rec_recall.ads_vova_bod_goods_rank_data_tmp;
RENAME table rec_recall.ads_vova_bod_goods_rank_data_bak to rec_recall.ads_vova_bod_goods_rank_data;
RENAME table rec_recall.ads_vova_bod_goods_rank_data_tmp to rec_recall.ads_vova_bod_goods_rank_data_bak;
"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
exit 1
fi
