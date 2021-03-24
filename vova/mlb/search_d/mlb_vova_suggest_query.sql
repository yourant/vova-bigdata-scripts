--create external table if not exists mlb.mlb_vova_suggest_query_d
--(
--   query    string comment '搜索query',
--   weight   int    comment '搜索人数'
--)comment '搜索词联想'
--partitioned by (pt string)
--ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
--STORED AS parquet
--LOCATION "s3://vova-mlb/REC/data/search/intention/mlb_vova_suggest_query_d"
--;


alter table mlb.mlb_vova_suggest_query_d drop if exists partition(pt='{pt}');
insert into mlb.mlb_vova_suggest_query_d partition(pt='{pt}')
select /*+ REPARTITION(5) */
   clk_from as query,
   count(distinct buyer_id)    weight
from mlb.mlb_vova_user_behave_link_d
where pt >= '{pt_before60}' and page_code='search_result' and length(clk_from)>1 and length(clk_from)<=60
group by clk_from
having weight >= 60
;