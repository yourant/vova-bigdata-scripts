#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="dwb_vova_search_no_result_req6212_chenkai_${cur_date}"

###逻辑sql
sql="
create table if not exists tmp.tmp_search_${table_suffix} as
  select
    datasource,
    os_type platform,
    lower(trim(regexp_replace(element_id,'\\\\\\\',''))) search_word,
    geo_country region_code,
    device_id,
    0 is_no_result
  from dwd.dwd_vova_log_common_click
  where pt='${cur_date}' and element_name='search_confirm'  and page_code in('search_begin','search_result')
    and element_id is not null and element_id !='' and os_type in('android','ios')
    and datasource is not null and geo_country is not null and platform is not null
  union all
  SELECT
  distinct -- 过滤重复的 search_word+device_id，如果一个用户搜索多个相同的词，现在区分不出来，只算一条
    datasource,
    os_type platform,
    lower(trim(regexp_replace(element_type,'\\\\\\\',''))) search_word, -- 去空格，转小写
    geo_country region_code,
    device_id,
    1 is_no_result
  FROM
    dwd.dwd_vova_log_impressions_arc
  WHERE pt ='${cur_date}'
    and page_code = 'search_result' and event_type = 'goods' and event_name='impressions'
    and (list_type = '/search_result_also_like' or extra like '%search_result_also_like%')
    and element_type is not null and element_type != '' and os_type in('android','ios')
    and datasource is not null and geo_country is not null and platform is not null
;

create table if not exists tmp.tmp_brand_word_${table_suffix} as
  select
    distinct lower(trim(brand_name)) brand_name
  from
  (
    select
      vb.brand_id,
      vb.brand_name,
      count(distinct(order_goods_id)) order_goods_cnt
    from
      ods_vova_vts.ods_vova_brand vb
    left join
      dim.dim_vova_order_goods dog
    on vb.brand_id = dog.brand_id
    where pay_status >=1 and pay_time >= date_sub('${cur_date}', 30) and parent_order_id =0
    group by vb.brand_id, vb.brand_name
  ) where order_goods_cnt > 10
;


insert overwrite table dwb.dwb_vova_search_no_result_frequent_word PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
  datasource,
  region_code,
  search_word,
  is_brand_word,
  search_pv,
  search_uv,
  search_no_result_pv,
  search_no_result_uv
from
(
  select
    * ,
    row_number() over(partition by datasource, region_code, search_word order by word_matching_rate desc) row
  from (
    select
      datasource,
      region_code,
      search_word,
      case when word.brand_name is not null and (
                (result.search_word like concat('%',word.brand_name,'%') and length(brand_name) / length(search_word) >= 0.3) or  --
                (word.brand_name like concat('%',result.search_word,'%') and length(search_word) / length(brand_name) >= 0.5) --
                )
           then 'Y'
           else 'N' end is_brand_word,
      brand_name,
      search_pv,
      search_uv,
      search_no_result_pv,
      search_no_result_uv,
    case when word.brand_name is not null and result.search_word like concat('%',word.brand_name,'%') then length(brand_name) / length(search_word)
         when word.brand_name is not null and word.brand_name like concat('%',result.search_word,'%') then length(search_word) / length(brand_name)
         else 0
         end word_matching_rate
    from
    (
      select
        tmp.*,
        row_number() over(partition by datasource, region_code order by search_no_result_pv desc) row
      from (
        select
          nvl(datasource, 'all') datasource,
          nvl(region_code, 'all') region_code,
          nvl(search_word, 'all') search_word,
          sum(if(is_no_result = 0, 1, 0)) search_pv,
          count(distinct(if(is_no_result = 0, device_id, null))) search_uv,
          sum(if(is_no_result = 1, 1, 0)) search_no_result_pv,
          count(distinct(if(is_no_result = 1, device_id, null))) search_no_result_uv
        from
        tmp.tmp_search_${table_suffix}
        group by cube(datasource, region_code, search_word)
        having region_code in ('all','FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW')
      ) tmp where search_no_result_pv >= 5
    ) result
    left join
    tmp.tmp_brand_word_${table_suffix} word
    on result.search_word like concat('%',word.brand_name,'%') or word.brand_name like concat('%',result.search_word,'%')
    where row <= 200
  )
) tmp where row = 1
;

insert overwrite table dwb.dwb_vova_search_no_result PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
nvl(datasource, 'all') datasource,
nvl(region_code, 'all') region_code,
nvl(platform, 'all') platform,
nvl(is_brand_word, 'all') is_brand_word,
count(distinct search_word) no_result_word_cnt,
sum(search_pv) search_pv,
count(distinct(if(search_pv > 0, device_id, null))) search_uv,
sum(search_no_result_pv) search_no_result_pv,
count(distinct(if(search_no_result_pv > 0, device_id, null))) search_no_result_uv
from
(
  select
    datasource,
    region_code,
    platform,
    search_word,
    device_id,
    search_pv,
    search_no_result_pv,
    case when word.brand_name is not null and (
        (result.search_word like concat('%',word.brand_name,'%') and length(brand_name) / length(search_word) >= 0.3) or  --
        (word.brand_name like concat('%',result.search_word,'%') and length(search_word) / length(brand_name) >= 0.5) --
      )
      then 'Y'
      else 'N' end is_brand_word,
    case when word.brand_name is not null and result.search_word like concat('%',word.brand_name,'%') then length(brand_name) /   length(search_word)
      when word.brand_name is not null and word.brand_name like concat('%',result.search_word,'%') then length(  search_word) / length(brand_name)
      else 0
      end word_matching_rate,
    row_number() over(partition by datasource, region_code, platform, search_word, device_id order by (
        case when word.brand_name is not null and result.search_word like concat('%',word.brand_name,'%')
             then length(brand_name) / length(search_word)
             when word.brand_name is not null and word.brand_name like concat('%',result.search_word,'%')
             then length(search_word) / length(brand_name)
             else 0
        end -- word_matching_rate
      ) desc) row
  from
    (
    select
    /*+ REPARTITION(50) */
      datasource,
      region_code,
      platform,
      search_word,
      device_id,
      sum(if(is_no_result = 0, 1, 0)) search_pv,
      sum(if(is_no_result = 1, 1, 0)) search_no_result_pv
    from
      tmp.tmp_search_${table_suffix}
    group by datasource, region_code, platform, search_word, device_id
    having datasource in ('vova', 'airyclub')
    ) result
  left join
    tmp.tmp_brand_word_${table_suffix} word
  on result.search_word like concat('%',word.brand_name,'%') or word.brand_name like concat('%',result.search_word,'%')
) tmp1 where row = 1 and datasource is not null and region_code is not null and platform is not null and is_brand_word is not null
group by cube(datasource, region_code, platform, is_brand_word)
having region_code in ('all','FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW') and datasource in ('vova', 'airyclub', 'all')
;

drop table if exists tmp.tmp_search_${table_suffix};
drop table if exists tmp.tmp_brand_word_${table_suffix};
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 15G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

