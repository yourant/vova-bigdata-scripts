insert overwrite table mlb.mlb_vova_suggest_query_d partition(pt='{pt}')
select /*+ REPARTITION(5) */
   clk_from as query,
   count(distinct buyer_id)    weight
from mlb.mlb_vova_user_behave_link_d
where pt >= '{pt_before60}' and page_code='search_result' and length(clk_from)>1 and length(clk_from)<=60
group by clk_from
having weight >= 60
;