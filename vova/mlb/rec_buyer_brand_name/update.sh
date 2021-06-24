#!/bin/bash
cur_date=$1
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

spark-sql   --conf "spark.app.name=mlb.mlb_vova_brand_like" --conf "spark.sql.crossJoin.enabled=true"   --conf "spark.dynamicAllocation.maxExecutors=120"  -e "
insert overwrite table mlb.mlb_vova_search_words_top
select brand_name,row_number() over (order by gmv desc) rn
from (
         select c.brand_id,
                c.brand_name,
                sum(a.shop_price * a.goods_number + a.shipping_fee)                                   gmv
         from dwd.dwd_vova_fact_pay a
                  join dim.dim_vova_goods b on a.goods_id = b.goods_id
                  join ods_vova_vts.ods_vova_brand c on b.brand_id = c.brand_id
                  join dwd.dwd_vova_fact_order_cause_v2 d on a.order_goods_id = d.order_goods_id
         where to_date(a.pay_time) >= date_sub('${cur_date}', 6)
           and to_date(a.pay_time) <= '${cur_date}'
           and d.pre_page_code = 'search_result'
           and d.pre_list_type in ('/search_result', '/search_result_recommend')
         group by c.brand_id, c.brand_name
         order by gmv desc
         limit 500
     ) t
;

insert overwrite table mlb.mlb_vova_brand_like
select
a.buyer_id,b.brand_name,row_number() over (order by a.likes_weight_synth desc) rn
from (select buyer_id,brand_id,likes_weight_synth,row_number() over (partition by buyer_id order by likes_weight_synth desc) rn
from ads.ads_vova_buyer_portrait_brand_likes_exp a
where a.pt = '${cur_date}'
    ) a
join ods_vova_vts.ods_vova_brand b on a.brand_id = b.brand_id
join (select buyer_id from dwd.dwd_vova_fact_start_up a where a.pt >= date_sub('${cur_date}',180) and a.pt <= '${cur_date}' group by buyer_id) c on a.buyer_id = c.buyer_id
where  a.rn = 1
;
"

mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
drop table if exists rec_recall.vova_top500_brand_name_new;
drop table if exists rec_recall.vova_top500_brand_name_pre;
CREATE TABLE rec_recall.vova_top500_brand_name_new
(
    id int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    brand_name      varchar(128) COMMENT 'brand_name',
    rn      int COMMENT 'rank',
    cur_date TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)  COMMENT '日期',
    PRIMARY KEY (id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='top500_brand_name';
EOF



sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--table vova_top500_brand_name_new \
--m 1 \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_search_words_top \
--columns brand_name,rn \
--fields-terminated-by '\001'


if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
rename table rec_recall.vova_top500_brand_name to rec_recall.vova_top500_brand_name_pre;
rename table rec_recall.vova_top500_brand_name_new to rec_recall.vova_top500_brand_name;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
drop table if exists rec_recall.vova_buyer_brand_name_new;
drop table if exists rec_recall.vova_buyer_brand_name_pre;
CREATE TABLE rec_recall.vova_buyer_brand_name_new
(
    id int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    buyer_id      int(18) COMMENT 'query',
    brand_name      varchar(128) COMMENT '语言',
    rn      int(14) COMMENT 'rn',
    cur_date TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)  COMMENT '日期',
    PRIMARY KEY (id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='buyer_brand_name';
EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--table vova_buyer_brand_name_new \
--m 10 \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_brand_like \
--columns buyer_id,brand_name,rn \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
rename table rec_recall.vova_buyer_brand_name to rec_recall.vova_buyer_brand_name_pre;
rename table rec_recall.vova_buyer_brand_name_new to rec_recall.vova_buyer_brand_name;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi