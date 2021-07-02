SET hive.exec.compress.output=true;

with goods_impression as (
--impression
SELECT
    goods_id,
    project,
    country,
    platform_type,
    age_group,
    sum(goods_uv) as impressions
FROM tmp.goods_user_birthday
WHERE record_type = 'impression'
GROUP BY
    goods_id,
    project,
    country,
    platform_type,
    age_group
),

goods_click as (
-- click
    SELECT
        goods_id,
        project,
        country,
        platform_type,
        age_group,
        sum(goods_uv) as clicks
    FROM tmp.goods_user_birthday
    WHERE record_type = 'click'
    GROUP BY
        goods_id,
        project,
        country,
        platform_type,
        age_group
),
all_goods_impression as (
--impression
SELECT
    goods_id,
    project,
    country,
    age_group,
    sum(goods_uv) as impressions
FROM tmp.goods_user_birthday
WHERE record_type = 'impression'
GROUP BY
    goods_id,
    project,
    country,
    age_group
),
all_goods_click as (
-- click
    SELECT
        goods_id,
        project,
        country,
        age_group,
        sum(goods_uv) as clicks
    FROM tmp.goods_user_birthday
    WHERE record_type = 'click'
    GROUP BY
        goods_id,
        project,
        country,
        age_group
)

insert overwrite table ads.ads_fd_goods_age_group_target
select
/*+ REPARTITION(20) */
    gi.goods_id,
    gi.project,
    gi.country,
    gi.platform_type,
    gi.age_group,
    gc.clicks,
    gi.impressions
from goods_impression gi
full join goods_click gc
on gi.goods_id = gc.goods_id
and gi.project = gc.project
and gi.country = gc.country
and gi.platform_type=gc.platform_type
and gi.age_group = gc.age_group

union all

select
    gi.goods_id,
    gi.project,
    gi.country,
    'all' as platform_type,
    gi.age_group,
    gc.clicks,
    gi.impressions
from all_goods_impression gi
full join all_goods_click gc
on gi.goods_id = gc.goods_id
and gi.project = gc.project
and gi.country = gc.country
and gi.age_group = gc.age_group
;
