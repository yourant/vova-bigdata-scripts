#!/bin/bash
#指定日期和引擎
pre_hour=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_hour=`date -d "-168hour" "+%Y-%m-%d %H:00:00"`
fi
echo "$pre_hour "
#默认小时
pt=$2
if [ ! -n "$2" ];then
pt=`date -d "$pre_hour" +%Y-%m-%d`
fi
echo "$pt"

sql="
insert overwrite table ads.ads_vova_hot_search_word PARTITION (pt = '$pt')
select
/*+ REPARTITION(1) */
app_from,
language_id,
gender,
is_shield,
hot_word,
search_counts
from
(
select
app_from,
language_id,
gender,
is_shield,
hot_word,
search_counts,
row_number() over (partition by app_from,language_id,gender,is_shield,hot_word order by search_counts desc) row_num
from
(
select
app_from,
language_id,
gender,
is_shield,
hot_word,
count(distinct device_id) search_counts
from
(
select
gc.datasource app_from,
nvl(dl.languages_id,0) language_id,
case when gc.gender ='male' then 1 when gc.gender ='female' then 2 else 0 end gender,
case when gc.geo_country in ('CN','HK','IE','MY','NL','AN','PT','SG','US','TW') then 1 else 0  end is_shield,
lower(trim(element_type)) hot_word,
gc.device_id
from
dwd.dwd_vova_log_impressions_arc gc
left join dim.dim_vova_languages dl on gc.language=dl.languages_code
where pt>='$pt' and collector_ts >='$pre_hour' and event_type='goods' and page_code ='search_result' and list_type in ('/search_result_recommend','/search_result_sold','/search_result_price_desc','/search_result_price_asc','/search_result')
) t1 where hot_word is not null and hot_word !='' and language_id >0
group by app_from,language_id,gender,is_shield,hot_word
having search_counts>10
) t2
) t3 where row_num <=20
"
spark-sql  --conf "spark.app.name=ads_vova_hot_search_word_h_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=100"  -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
drop table if exists themis.hot_search_word_new;
drop table if exists themis.hot_search_word_pre;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS themis.hot_search_word_new (
  id int(11) NOT NULL AUTO_INCREMENT,
  app_from varchar(16) NOT NULL COMMENT 'ac|vova',
  gender int(11) NOT NULL COMMENT '性别,1:男性 2:女性 0:未知',
  language_id int(11) NOT NULL COMMENT '语言id',
  is_shield int(11) NOT NULL COMMENT '是否brand屏蔽 1:是 0：否',
  hot_word varchar(256) NOT NULL COMMENT '热搜词汇',
  search_counts int(11) NOT NULL COMMENT '搜索次数',
  create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (id),
  KEY app_from_gender_language_id_is_shield (app_from,gender,language_id,is_shield),
  KEY language_id (language_id) USING BTREE,
  KEY search_counts (search_counts) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='热搜词汇表';

CREATE TABLE IF NOT EXISTS themis.hot_search_word (
  id int(11) NOT NULL AUTO_INCREMENT,
  app_from varchar(16) NOT NULL COMMENT 'ac|vova',
  gender int(11) NOT NULL COMMENT '性别,1:男性 2:女性 0:未知',
  language_id int(11) NOT NULL COMMENT '语言id',
  is_shield int(11) NOT NULL COMMENT '是否brand屏蔽 1:是 0：否',
  hot_word varchar(256) NOT NULL COMMENT '热搜词汇',
  search_counts int(11) NOT NULL COMMENT '搜索次数',
  create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (id),
  KEY app_from_gender_language_id_is_shield (app_from,gender,language_id,is_shield),
  KEY language_id (language_id) USING BTREE,
  KEY search_counts (search_counts) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='热搜词汇表';
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis?useUnicode=true\&characterEncoding=utf-8 \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--table hot_search_word_new \
--m 1 \
--columns app_from,language_id,gender,is_shield,hot_word,search_counts \
--hcatalog-database ads \
--hcatalog-table ads_vova_hot_search_word \
--hcatalog-partition-keys pt \
--hcatalog-partition-values $pt \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
#rename table themis.hot_search_word to themis.hot_search_word_pre;
#rename table themis.hot_search_word_new to themis.hot_search_word;
echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table themis.hot_search_word to themis.hot_search_word_pre,themis.hot_search_word_new to themis.hot_search_word;
EOF
echo "-------rename结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


