--根据用户历史搜索行为，构建热搜词词典
--每天定时运行

--1. 拿取query, 对query原始数据进行预处理, 明细数据，存储
drop table if exists tmp.tmp_vova_search_query_cnt;
create table tmp.tmp_vova_search_query_cnt as
select /*+ REPARTITION(10) */

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
;

----2. 分词，转列，计算词与性别词汇levenshtein距离
--drop table if exists tmp.tmp_vova_intent_query_gender;
--create table tmp.tmp_vova_intent_query_gender as
--select
--   C.query   as query,
--   case when D.gender is null then '2'
--        when D.gender is not null and length(C.word)<=4 and C.word != D.word then '2'
--        else D.gender end   as query_gender
--from(
--   select
--      query,
--      word
--   from (select distinct query from tmp.tmp_vova_intent_search_query) A
--   lateral view explode(split(regexp_replace(query,'[\\`~!@#$%^&*()_\\+=|{}\"\\:\\;,\\[\\].<>/?~！@#￥%……&*（）+|【】‘；：”“’。，、？\\t]',' '),' ')) B as word
--   where length(word)>2 and word not regexp '^[0-9]+&'
--)C
--left join mlb.mlb_vova_gender_dic D on levenshtein(C.word, D.word)<=2
--group by C.query,
--   case when D.gender is null then '2'
--        when D.gender is not null and length(C.word)<=4 and C.word != D.word then '2'
--                   else D.gender end
--;

----3. ads.ads_query_cnt_d 与 tmp.tmp_query_gender进行join，获取query gender
--drop table if exists tmp.tmp_vova_query_ctr;
--create table tmp.tmp_vova_query_ctr as
--select /*+ REPARTITION(10) */
--   A.query         as query,
--   A.user_gender   as user_gender,
--   B.query_gender  as query_gender,
--   A.cat_id        as cat_id,
--   A.expre_cnt     as expre_cnt,
--   A.clk_cnt       as clk_cnt,
--   (A.clk_cnt+2)/(A.expre_cnt+100)   as ctr
--from tmp.tmp_vova_intent_search_query  A
--join tmp.tmp_vova_intent_query_gender B on A.query=B.query
--;

-- e. 分词，统计各个词的曝光次数与点击次数
drop table if exists tmp.tmp_vova_word_cnt;
create table tmp.tmp_vova_word_cnt as
select /*+ REPARTITION(10) */
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
      from tmp.tmp_vova_search_query_cnt
   ) A
   lateral view explode(split(word_arr,' ')) B as word
   where length(word) > 1
   group by word, cat_id, gender
) C
left join mlb.mlb_vova_gender_dic D on C.word=D.word
;

-- 统计每个词的曝光平均次数
drop table if exists tmp.tmp_vova_word_cat_avg;
create table tmp.tmp_vova_word_cat_avg as
select /*+ REPARTITION(1) */
   word, case when avg_expre_cnt<1000 then 1000 else avg_expre_cnt end    as avg_expre_cnt
from(
   select
      word,
      avg(expre_cnt)  as avg_expre_cnt
   from tmp.tmp_vova_word_cnt
   where query_gender = '2'
   group by word
)A
;

--统计各个cat_id中query gender的平均次数
drop table if exists tmp.tmp_vova_query_gender_cat_bs;
create table tmp.tmp_vova_query_gender_cat_bs as
select /*+ REPARTITION(1) */
   cat_id, case when expre_cnt<1000 then 1000 else expre_cnt end bs_para
from(
   select cat_id,  avg(expre_cnt)  as expre_cnt
   from tmp.tmp_vova_word_cnt where user_gender='0' or user_gender='1'
   group by cat_id
)T
;

--统计各个类别中用户基本信息中性别平均次数
drop table if exists tmp.tmp_vova_user_gender_cat_bs;
create table tmp.tmp_vova_user_gender_cat_bs as
select /*+ REPARTITION(1) */
   cat_id,
   case when expre_cnt<1000 then 1000 else expre_cnt end bs_para
from(
   select cat_id,  avg(expre_cnt)  as expre_cnt
   from tmp.tmp_vova_word_cnt where query_gender='0' or query_gender='1'
   group by cat_id
)T
;

--计算query各个品类不同性别的点击几率
drop table if exists tmp.tmp_vova_query_gender_cat_ctr;
create table tmp.tmp_vova_query_gender_cat_ctr as
select /*+ REPARTITION(1) */
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
   from tmp.tmp_vova_word_cnt
   group by query_gender, cat_id
)A
join tmp.tmp_vova_query_gender_cat_bs B on A.cat_id = B.cat_id
;

--计算各个品类用户基本信息性别的点击几率
drop table if exists tmp.tmp_vova_user_gender_cat_ctr;
create table tmp.tmp_vova_user_gender_cat_ctr as
select /*+ REPARTITION(1) */
   A.user_gender  as gender,
   A.cat_id       as cat_id,
   A.expre_cnt    as expre_cnt,
   A.clk_cnt      as clk_cnt,
   (clk_cnt+bs_para*0.001)/(expre_cnt+bs_para)  as ctr
from (
   select user_gender, cat_id,
      sum(expre_cnt)  as expre_cnt,
      sum(clk_cnt)    as clk_cnt
   from tmp.tmp_vova_word_cnt
   group by user_gender, cat_id
)A
join tmp.tmp_vova_user_gender_cat_bs B on A.cat_id=B.cat_id
;

--计算性别-品类概率
drop table if exists tmp.tmp_vova_cat_gender_score;
create table tmp.tmp_vova_cat_gender_score as
select /*+ REPARTITION(1) */
   nvl(A.cat_id, B.cat_id)   as cat_id,
   nvl(A.gender, B.gender)   as gender,
   nvl(A.ctr, 0.001)*0.3 + nvl(B.ctr, 0.001)*0.7
                             as score
from tmp.tmp_vova_user_gender_cat_ctr   A
full join tmp.tmp_vova_query_gender_cat_ctr B on A.gender=B.gender and A.cat_id=B.cat_id
;

drop table if exists tmp.tmp_vova_cat_gender_prob;
create table tmp.tmp_vova_cat_gender_prob as
select /*+ REPARTITION(1) */
   A.cat_id   as cat_id,
   A.gender   as gender,
   round(A.score/B.score_sum,4)   as prob
from tmp.tmp_vova_cat_gender_score A
join (
   select
      cat_id, sum(score)  as score_sum
   from tmp.tmp_vova_cat_gender_score
   where gender = '0' or gender = '1'
   group by cat_id
) B on A.cat_id=B.cat_id where A.gender = '0' or A.gender = '1'
;

--create external table if not exists mlb.mlb_vova_word_prob
--(
--   key         string comment 'word or gender',
--   type        string comment '0: word, 1: gender',
--   cat_id      bigint comment '品类',
--   expre_cnt   bigint comment '曝光次数',
--   clk_cnt     bigint comment '点击次数',
--   prob        double comment '概率'
--) comment '搜索词汇-品类概率表'
--partitioned by (pt string)
--ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
--STORED AS parquet
--LOCATION "s3://vova-mlb/REC/data/search/intention/mlb_vova_word_prob"
--;


--f. 统计词在每个品类下的贝叶斯平均ctr, 作为贝叶斯概率P(cat_id|word)
alter table mlb.mlb_vova_word_prob drop if exists partition(pt='{pt}');
insert into mlb.mlb_vova_word_prob partition(pt='{pt}')
select /*+ REPARTITION(10) */
   key, type, cat_id, expre_cnt, clk_cnt, prob*100 as prob
from(
    select
       t1.word     as key,
       '0'         as type,
       cat_id, expre_cnt, clk_cnt,
       round((clk_cnt+avg_expre_cnt*0.01)/(expre_cnt+avg_expre_cnt),4)   as prob
    from tmp.tmp_vova_word_cnt      t1
    join tmp.tmp_vova_word_cat_avg  t2 on t1.word=t2.word
    where expre_cnt > 20 and clk_cnt > 10 and query_gender = '2'
)A
union
select
   gender as key,
   '1'    as type,
   cat_id, 0 as expre_cnt, 0 as clk_cnt,
   case when prob>0.62 then 0.9
        when prob<0.48 then 0.1
        else 0.5 end         as prob
from tmp.tmp_vova_cat_gender_prob
--测试，<用户基性别：男性，query：'nike'>, 查看prob分布情况, 若品类top系列为男性的nike鞋、衣服等，则可行
;

create external table if not exists mlb.mlb_vova_word_prob_json
(
   key         string comment 'word or gender',
   type        int comment '0: word, 1: gender',
   cat_info    string comment 'cat_id:prob,cat_id:prob,...'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet
LOCATION "s3://vova-mlb/REC/data/search/intention/mlb_vova_word_prob_json"
;

insert overwrite table mlb.mlb_vova_word_prob_json
select /*+ REPARTITION(5) */
   key,
   cast(type as int) type,
   concat_ws(',', collect_set(concat(cat_id,':', cast(prob as decimal(15,4))))) as cat_info
from mlb.mlb_vova_word_prob
where pt='{pt}' and key not regexp '^\\[~!@#$%^&*()-_+=\\\'<>?]+'
group by key, cast(type as int)
;