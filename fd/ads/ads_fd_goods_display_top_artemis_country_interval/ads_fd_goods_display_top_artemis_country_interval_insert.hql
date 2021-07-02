SET hive.exec.compress.output=true;

with goods_impression as (
--impression
SELECT
/*+ REPARTITION(10) */
    goods_id,
    cat_id,
    country,
    project,
    platform,
    sum(goods_uv) as impressions,
    case when project in ('tendaisy','sisdress','poprhine','beautlly','trendaisy','herachoice','blessrose','shinynight','jollyweek','merecloth','baltershop','eoschoice','chichut','cherbow','cherlady','cosydress','joycedays','vividpretty')
    and platform in ('web','h5') then '1'
    when project in ('floryday','airydress') and platform in ('web','h5', 'mob') then '1'
    else '0' end as mark_code
FROM tmp.tmp_fd_goods_uv_interval
WHERE record_type = 'impression' and list_type in ('list-category', "list-pre-order") and goods_id is not null and goods_id != ''
GROUP BY
    goods_id,
    cat_id,
    country,
    project,
    platform
),

tmp_fd_country_top as (
select
    *,
    row_number() over (partition by cat_id,country order by impressions desc) as rank
from
    goods_impression
where mark_code = '1' and goods_id != 'undefined' and goods_id != 'favorites' and length(country) < 3
)

insert overwrite table ads.ads_fd_goods_display_top_artemis_country_interval
select
    /*+ REPARTITION(10) */
    goods_id,
    cat_id,
    country as country_code,
    project as project_name,
    platform,
    cast('${pt_begin} 16:00:00' as timestamp) as start_time,
    cast('${pt_end} 16:00:00' as timestamp) as end_time,
    cast('${pt}' as int) as interval,
    1 as is_active
from
    tmp_fd_country_top
where
    rank <= 60;
