#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
                  cur_date=$(date -d "-1 day" +%Y-%m-%d)
fi

mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "
drop table if exists rec_recall.ads_vova_rec_m_tagcombine_d_bak;
CREATE TABLE IF NOT EXISTS rec_recall.ads_vova_rec_m_tagcombine_d_bak (
id int(11) NOT NULL AUTO_INCREMENT,
region_id int(11) NOT NULL COMMENT '区域',
gender VARCHAR(10) NOT NULL COMMENT '性别',
user_age_group VARCHAR(100) NOT NULL COMMENT '用户年龄组',
kg_tag_combine VARCHAR(100) NOT NULL COMMENT '标签组合',
goods_id int(11) NOT NULL COMMENT '商品id',
rank int(11) NOT NULL COMMENT '商品排名',
tag_score double(20,20) NOT NULL COMMENT '标签打分',
bod_id int(11) NOT NULL COMMENT '榜单id',
update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (id) USING BTREE,
KEY region_id (region_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱-偏好召回';
"

if [ $? -ne 0 ];then
          exit 1
fi
reg='\\|'
sql="
msck repair table mlb.mlb_rec_m_tagcombine_d;
insert overwrite table ads.ads_vova_rec_m_tagcombine_d partition (pt = '${cur_date}')
select region_id,gender,user_age_group,kg_tag_combine,goods_id,rank,tag_score,bod_id from (
SELECT
region_id,gender,user_age_group,t1.kg_tag_combine,t2.goods_id,rank() over(partition by t1.kg_tag_combine order by t2.overall_score desc) as rank,
t1.tag_score,t1.bod_id
from
(select region_id,gender,user_age_group,kg_tag_combine,max(tag_score) as tag_score,
split(kg_tag_combine,'${reg}')[0] as v1,
split(kg_tag_combine,'${reg}')[1] as v2,
split(kg_tag_combine,'${reg}')[2] as second_cat_id,
bod_id
from mlb.mlb_rec_m_tagcombine_d where pt='${cur_date}'
GROUP by region_id,gender,user_age_group,kg_tag_combine,bod_id) t1
left join (
select
a.goods_id,
b.v_str,
b.second_cat_id,
a.overall_score
from
mlb.mlb_vova_rec_b_goods_score_d a
LEFT JOIN tmp.tmp_vova_attribute_data_group b on a.goods_id=b.goods_id
where a.pt='${cur_date}' and a.overall_score>27 and b.v_str is not null
) t2 on t1.second_cat_id=t2.second_cat_id
where t2.v_str like concat('%',t1.v1,'%') and t2.v_str like concat('%',t1.v2,'%')
) T WHERE rank<=500
"
spark-sql --conf "spark.app.name=ads_vova_tagcombine" --conf "spark.dynamicAllocation.maxExecutors=150" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--m 1 \
--table ads_vova_rec_m_tagcombine_d_bak \
--hcatalog-database ads \
--hcatalog-table ads_vova_rec_m_tagcombine_d \
--hive-partition-key pt \
--hive-partition-value ${cur_date} \
--columns region_id,gender,user_age_group,kg_tag_combine,goods_id,rank,tag_score,bod_id \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
exit 1
fi

mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "
RENAME table rec_recall.ads_vova_rec_m_tagcombine_d to rec_recall.ads_vova_rec_m_tagcombine_d_tmp;
RENAME table rec_recall.ads_vova_rec_m_tagcombine_d_bak to rec_recall.ads_vova_rec_m_tagcombine_d;
RENAME table rec_recall.ads_vova_rec_m_tagcombine_d_tmp to rec_recall.ads_vova_rec_m_tagcombine_d_bak;
"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
exit 1
fi
