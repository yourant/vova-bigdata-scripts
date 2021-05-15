#!/bin/bash
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
pt_start=`date -d "30 days ago ${pt}" +%Y-%m-%d`
echo $pt
echo $pt_start

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" \
--conf "spark.sql.adaptive.shuffle.targetPostShuffleInputSize=128000000" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=vova_mlb_search_correct_gram_d" -e "

with goods_name_extract as (
        select
            word, count(distinct goods_sn) as weight
        from(
            select
               concat_ws(' ',sentences(lower(regexp_replace(goods_name, '\\\n|\\;|!|\\\?|？|！', ' ')))[0]) as goods_name,
               goods_sn
            from (SELECT DISTINCT goods_sn, goods_name FROM ads.ads_vova_goods_portrait where pt='${pt}' and is_on_sale=1)t
        )t
        lateral view explode(split(goods_name, ' ')) wo as word
        group by word
        having weight>=2000
        ),
    search_query as(
        select
            word, sum(weight) weight
        from (
            select
                concat_ws(' ',sentences(clk_from)[0]) as query, count(distinct session_id, buyer_id, device_id) as weight
            from mlb.mlb_vova_user_behave_link_d
            where pt>='${pt_start}' and pt<='${pt}' and page_code='search_result' and clk_from is not null and length(clk_from)>1
            group by clk_from
            having weight >= 100
        )t
        lateral view explode(split(query, ' ')) wo as word
        group by word
    ),
    correct_word as(
        select DISTINCT
            nvl(t1.word, t2.word) as word
        from(select distinct word from goods_name_extract where length(word)>1 and word not rlike '^\\d+$') t1
        full join (select distinct word from search_query where length(word)>1 and word not rlike '^\\d+$') t2
        on t1.word = t2.word
    )
    insert overwrite table mlb.mlb_vova_search_correct_word_d partition(pt='${pt}')
    select /*+ REPARTITION(1) */ word from correct_word where word not rlike '[\\u4e00-\\u9fa5]'
    union select /*+ REPARTITION(1) */ word from mlb.mlb_vova_search_manual_word
;

with goods_name as (
        select distinct
           concat_ws(' ',sentences(lower(regexp_replace(goods_name, '\\\n|\\;|!|\\\?|？|！', ' ')))[0]) as goods_name
        from (SELECT DISTINCT goods_sn, goods_name FROM ads.ads_vova_goods_portrait where pt='${pt}' and is_on_sale=1)t
    ),
    goods_name_pos as(
        select distinct
            t1.goods_name, t1.pos, t1.word
        from(
            select
                goods_name,
                word.pos    as pos,   --词的位置
                word.word   as word   --词
            from goods_name
            lateral view posexplode(split(goods_name, '[ ,]+')) word as pos, word
        ) t1
        --join mlb.mlb_vova_search_correct_word_d t2 on t2.pt='${pt}' and t1.word = t2.word
    ),
    goods_word_stat as (
       select word, count(distinct goods_name) as cnt from goods_name_pos group by word
    ),
    n_gram_name_raw_data as(
        select
            t1.word         as src_word,
            t2.word         as dist_word,
            0.01*count(1)   as cnt
        from goods_name_pos t1
        join goods_name_pos t2 on t1.goods_name=t2.goods_name and t1.pos+1 = t2.pos
        group by t1.word, t2.word
        having cnt >= 100
    ),
    search_query as(
        select
            concat_ws(' ',sentences(clk_from)[0]) as query,
            count(distinct session_id, buyer_id, device_id) as cnt
        from mlb.mlb_vova_user_behave_link_d
        where pt>='${pt_start}' and pt<='${pt}' and page_code='search_result' and
              clk_from is not null and length(clk_from)>1
        group by clk_from
        having cnt >= 100
    ),

    query_pos as(
        select
            t1.query, t1.pos, t1.word, t1.cnt
        from(
            select
                query, pos, word, cnt
            from search_query
            lateral view posexplode(split(query, '[ ,]+')) word as pos, word
        )t1
        --join mlb.mlb_vova_search_correct_word_d t2 on t2.pt='${pt}' and t1.word = t2.word
    ),
    query_word_stat as (
        select word, sum(cnt) as cnt from query_pos group by word
    ),
    n_gram_query_raw_data as(
        select
            t1.word as src_word, t2.word as dist_word, sum(t1.cnt) as cnt
        from query_pos t1
        join query_pos t2 on t1.query = t2.query and t1.pos + 1 = t2.pos
        group by t1.word, t2.word
    ),
    n_gram_raw_data as(
        select
            nvl(t1.src_word, t2.src_word)   as src_word,
            nvl(t1.dist_word, t2.dist_word) as dist_word,
            nvl(t1.cnt, 0) + nvl(t2.cnt, 0) as cnt
        from n_gram_name_raw_data t1
        full join n_gram_query_raw_data t2 on t1.src_word=t2.src_word and t1.dist_word=t2.dist_word
        where nvl(t1.src_word, t2.src_word) not in ('i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', 'couldn', 'didn', 'doesn', 'hadn', 'hasn', 'haven', 'isn', 'ma', 'mightn', 'mustn', 'needn', 'shan', 'shouldn', 'wasn', 'weren', 'won', 'wouldn')
          and nvl(t1.dist_word, t2.dist_word) not in ('i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', 'couldn', 'didn', 'doesn', 'hadn', 'hasn', 'haven', 'isn', 'ma', 'mightn', 'mustn', 'needn', 'shan', 'shouldn', 'wasn', 'weren', 'won', 'wouldn')
    ),
    n_gram_group_data as(
        select
            src_word, sum(cnt) as all_cnt
        from n_gram_raw_data
        group by src_word
    ),

    word_data as (
        select
            nvl(t1.word, t2.word) as word,
            0.001*nvl(t1.cnt, 0) + 0.999*nvl(t2.cnt, 0)  as cnt   --优先取query，降低goods_name中的权重
        from goods_word_stat t1 full join query_word_stat t2 on t1.word=t2.word
    ),
    word_data_stat as (
        select
            max(cnt)   as max_cnt,
            min(cnt)   as min_cnt
        from word_data
    ),

    n_gram_data as(
        select
            src_word, dist_word, molecular, denominator, prob
        from (
            select
                t1.src_word,
                t1.dist_word,
                t1.cnt as molecular,
                t2.all_cnt as denominator, round(t1.cnt/t2.all_cnt, 4) as prob
            from n_gram_raw_data   t1
            join n_gram_group_data t2 on t1.src_word = t2.src_word
        )t1 where prob>0.001 and denominator>=100
        union all
        select
            src_word, dist_word, molecular, denominator, prob
        from
        (
            select
                t1.word                     as src_word,
                ''                          as dist_word,
                nvl(t2.cnt, t3.min_cnt)     as molecular,
                t3.max_cnt                  as denominator,
                round(nvl(t2.cnt, t3.min_cnt)/t3.max_cnt, 4)   as prob
            from (select word from mlb.mlb_vova_search_correct_word_d where pt='${pt}')  t1
            left join word_data       t2 on t1.word=t2.word
            cross join word_data_stat t3
        )t
    )
    insert overwrite table mlb.mlb_vova_search_correct_gram_d partition(pt = '${pt}')
    select /*+ REPARTITION(1) */ src_word, dist_word,molecular,denominator, prob from n_gram_data
;
"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

