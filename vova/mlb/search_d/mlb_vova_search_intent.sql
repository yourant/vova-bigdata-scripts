--根据用户历史搜索行为，构建热搜词词典
--每天定时运行

--1. 拿取query, 对query原始数据进行预处理, 明细数据，存储
with tmp_vova_search_query_cnt as
(
select
   clk_from as query,
   case when gender='female' then '0' when gender='male' then '1' else '2' end as gender,
   cat_id, first_cat_id, second_cat_id,
   count(1)        as expre_cnt,
   sum(is_click)   as clk_cnt
from mlb.mlb_vova_user_behave_link_d
where pt>='{pt_before30}' and pt<='{pt}' and clk_from is not null and length(clk_from)>1 and length(clk_from)<=60
group by
   clk_from, case when gender='female' then '0' when gender='male' then '1' else '2' end,
   cat_id, first_cat_id, second_cat_id
),

-- e. 分词，统计各个词的曝光次数与点击次数
tmp_vova_word_cnt as
(
select 
   C.word       as word,
   C.gender     as user_gender,
   case when D.word is not null then D.gender else '2' end   as query_gender,
   C.cat_id     as cat_id,
   c.expre_cnt  as expre_cnt,
   c.clk_cnt    as clk_cnt
from(
   select
      word, cat_id, gender,
      sum(expre_cnt)   expre_cnt,
      sum(clk_cnt)     clk_cnt
   from (
      select regexp_replace(query,'[\\`~!@#$%^&*()_\\+=|{}\"\\:\\;,\\[\\].<>/?~！@#￥%……&*（）+|【】‘；：”“’。，、？\\t]',' ')  as word_arr, 
         gender, cat_id, expre_cnt, clk_cnt
      from tmp_vova_search_query_cnt
   ) A
   lateral view explode(split(word_arr,' ')) B as word
   where length(word) > 1
   group by word, cat_id, gender
) C
left join mlb.mlb_vova_gender_dic D on C.word=D.word
),

-- 统计每个词的曝光平均次数
tmp_vova_word_cat_avg as
(
select
   word, case when avg_expre_cnt<1000 then 1000 else avg_expre_cnt end    as avg_expre_cnt
from(
   select
      word,
      avg(expre_cnt)  as avg_expre_cnt
   from tmp_vova_word_cnt
   where query_gender = '2'
   group by word
)A
),

--统计各个cat_id中query gender的平均次数
tmp_vova_query_gender_cat_bs as
(
select 
   cat_id, case when expre_cnt<1000 then 1000 else expre_cnt end bs_para
from(
   select cat_id,  avg(expre_cnt)  as expre_cnt
   from tmp_vova_word_cnt where user_gender='0' or user_gender='1'
   group by cat_id
)T
),

--统计各个类别中用户基本信息中性别平均次数
tmp_vova_user_gender_cat_bs as
(
select 
   cat_id,
   case when expre_cnt<1000 then 1000 else expre_cnt end bs_para
from(
   select cat_id,  avg(expre_cnt)  as expre_cnt
   from tmp_vova_word_cnt where query_gender='0' or query_gender='1'
   group by cat_id
)T
),

--计算query各个品类不同性别的点击几率
tmp_vova_query_gender_cat_ctr as
(
select 
   A.gender      as gender,
   A.cat_id      as cat_id,
   A.expre_cnt   as expre_cnt,
   A.clk_cnt     as clk_cnt,
   (clk_cnt+bs_para*0.001)/(expre_cnt+bs_para)   as ctr
from (
   select
      cast(query_gender as string)   as gender, cat_id,
      sum(expre_cnt)   as expre_cnt,
      sum(clk_cnt)     as clk_cnt
   from tmp_vova_word_cnt
   group by query_gender, cat_id
)A
join tmp_vova_query_gender_cat_bs B on A.cat_id = B.cat_id
),

--计算各个品类用户基本信息性别的点击几率
tmp_vova_user_gender_cat_ctr as
(
select 
   A.user_gender  as gender,
   A.cat_id       as cat_id,
   A.expre_cnt    as expre_cnt,
   A.clk_cnt      as clk_cnt,
   (clk_cnt+bs_para*0.001)/(expre_cnt+bs_para)  as ctr
from (
   select user_gender, cat_id,
      sum(expre_cnt)  as expre_cnt,
      sum(clk_cnt)    as clk_cnt
   from tmp_vova_word_cnt
   group by user_gender, cat_id
)A
join tmp_vova_user_gender_cat_bs B on A.cat_id=B.cat_id
),

--计算性别-品类概率
tmp_vova_cat_gender_score as
(
select
   nvl(A.cat_id, B.cat_id)   as cat_id,
   nvl(A.gender, B.gender)   as gender,
   nvl(A.ctr, 0.001)*0.3 + nvl(B.ctr, 0.001)*0.7
                             as score
from tmp_vova_user_gender_cat_ctr   A
full join tmp_vova_query_gender_cat_ctr B on A.gender=B.gender and A.cat_id=B.cat_id
),

tmp_vova_cat_gender_prob as
(
select 
   A.cat_id   as cat_id,
   A.gender   as gender,
   round(A.score/B.score_sum,4)   as prob
from tmp_vova_cat_gender_score A
join (
   select
      cat_id, sum(score)  as score_sum
   from tmp_vova_cat_gender_score
   where gender = '0' or gender = '1'
   group by cat_id
) B on A.cat_id=B.cat_id where A.gender = '0' or A.gender = '1'
)
--f. 统计词在每个品类下的贝叶斯平均ctr, 作为贝叶斯概率P(cat_id|word)
insert overwrite table mlb.mlb_vova_word_prob partition(pt='{pt}')
select /*+ REPARTITION(4) */
   key, type, cat_id, expre_cnt, clk_cnt, prob*100 as prob
from(
    select
       t1.word     as key,
       '0'         as type,
       cat_id, expre_cnt, clk_cnt,
       round((clk_cnt+avg_expre_cnt*0.01)/(expre_cnt+avg_expre_cnt),4)   as prob
    from tmp_vova_word_cnt      t1
    join tmp_vova_word_cat_avg  t2 on t1.word=t2.word
    where expre_cnt > 20 and clk_cnt > 10 and query_gender = '2'
)A
union
select /*+ REPARTITION(4) */
   gender as key,
   '1'    as type,
   cat_id, 0 as expre_cnt, 0 as clk_cnt,
   case when prob>0.62 then 0.9
        when prob<0.48 then 0.1
        else 0.5 end         as prob
from tmp_vova_cat_gender_prob
--测试，<用户基性别：男性，query：'nike'>, 查看prob分布情况, 若品类top系列为男性的nike鞋、衣服等，则可行
;

insert overwrite table mlb.mlb_vova_word_prob_json
select /*+ REPARTITION(5) */
   key,
   cast(type as int) type,
   concat_ws(',', collect_set(concat(cat_id,':', cast(prob as decimal(15,4))))) as cat_info
from mlb.mlb_vova_word_prob
where pt='{pt}' and key not regexp "^\\[~!@#$%^&*()-_+=\\\'<>?]+"
group by key, cast(type as int);