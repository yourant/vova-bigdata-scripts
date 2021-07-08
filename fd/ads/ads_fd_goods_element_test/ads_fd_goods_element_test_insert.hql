SET hive.exec.compress.output=true;

with goods_impression_picture as (
--impression
SELECT
/*+ REPARTITION(10) */
    goods_id,
    project,
    platform,
    country,
    picture_group,
    picture_batch,
    sum(uv) as impression_session
FROM tmp.tmp_fd_goods_picture_test
WHERE rtype = 'impression'
GROUP BY
    goods_id,
    project,
    platform,
    country,
    picture_group,
    picture_batch
),

goods_click_picture as (
-- click
SELECT
/*+ REPARTITION(10) */
    goods_id,
    project,
    platform,
    country,
    picture_group,
    picture_batch,
    sum(uv) as click_session
FROM tmp.tmp_fd_goods_picture_test
WHERE rtype = 'click'
GROUP BY
    goods_id,
    project,
    platform,
    country,
    picture_group,
    picture_batch
),

goods_impression as (
--impression
SELECT
/*+ REPARTITION(10) */
    goods_id,
    project,
    platform,
    country,
    element_tag,
    element_batch,
    sum(uv) as session_common_impression
FROM tmp.tmp_fd_goods_element_test
WHERE rtype = 'impression'
GROUP BY
    goods_id,
    project,
    platform,
    country,
    element_tag,
    element_batch
),

goods_click as (
-- click
SELECT
/*+ REPARTITION(10) */
    goods_id,
    project,
    platform,
    country,
    element_tag,
    element_batch,
    sum(uv) as session_common_click
FROM tmp.tmp_fd_goods_element_test
WHERE rtype = 'click'
GROUP BY
    goods_id,
    project,
    platform,
    country,
    element_tag,
    element_batch
),

goods_views as (
-- click
SELECT
/*+ REPARTITION(10) */
    goods_id,
    project,
    platform,
    country,
    element_tag,
    element_batch,
    sum(uv) as views
FROM tmp.tmp_fd_goods_element_test
WHERE rtype = 'impression' and trim(element_name) in ('add_to_cart', 'benefits_add_to_cart', 'direct_add_to_cart', 'goods_detail_add')
GROUP BY
    goods_id,
    project,
    platform,
    country,
    element_tag,
    element_batch
),

goods_cart as (
-- click
SELECT
/*+ REPARTITION(10) */
    goods_id,
    project,
    platform,
    country,
    element_tag,
    element_batch,
    sum(uv) as cart
FROM tmp.tmp_fd_goods_element_test
WHERE rtype = 'click' and trim(element_name) in ('add_to_cart', 'benefits_add_to_cart', 'direct_add_to_cart', 'goods_detail_add')
GROUP BY
    goods_id,
    project,
    platform,
    country,
    element_tag,
    element_batch
),

goods_video_impression as (
-- click
SELECT
/*+ REPARTITION(10) */
    goods_id,
    project,
    platform,
    country,
    element_tag,
    element_batch,
    sum(uv) as video_impression
FROM tmp.tmp_fd_goods_element_test
WHERE rtype = 'impression' and trim(element_name) ='play_video'
GROUP BY
    goods_id,
    project,
    platform,
    country,
    element_tag,
    element_batch
),

goods_video_play as (
-- click
SELECT
/*+ REPARTITION(10) */
    goods_id,
    project,
    platform,
    country,
    element_tag,
    element_batch,
    sum(uv) as video_play
FROM tmp.tmp_fd_goods_element_test
WHERE rtype = 'click' and trim(element_name) = 'play_video'
GROUP BY
    goods_id,
    project,
    platform,
    country,
    element_tag,
    element_batch
),

tmp_table_one as (
select
/*+ REPARTITION(10) */
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform,gc.platform) as platform,
    nvl(gi.country,gc.country) as country,
    nvl(gi.element_tag,gc.element_tag) as element_tag,
    nvl(gi.element_batch,gc.element_batch) as element_batch,
    nvl(gi.session_common_impression, 0) as session_common_impression,
    nvl(gc.session_common_click,0) as session_common_click
from goods_impression gi
full join goods_click gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.platform=gc.platform and gi.country=gc.country
and gi.element_batch=gc.element_batch and gi.element_tag=gc.element_tag
),

tmp_table_two as (
select
/*+ REPARTITION(10) */
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform,gc.platform) as platform,
    nvl(gi.country,gc.country) as country,
    nvl(gi.element_tag,gc.element_tag) as element_tag,
    nvl(gi.element_batch,gc.element_batch) as element_batch,
    nvl(gi.session_common_impression, 0) as session_common_impression,
    nvl(gi.session_common_click,0) as session_common_click,
    nvl(gc.views,0) as views
from tmp_table_one gi
full join goods_views gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.platform=gc.platform and gi.country=gc.country
and gi.element_batch=gc.element_batch and gi.element_tag=gc.element_tag
),

tmp_table_three as (
select
/*+ REPARTITION(10) */
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform,gc.platform) as platform,
    nvl(gi.country,gc.country) as country,
    nvl(gi.element_tag,gc.element_tag) as element_tag,
    nvl(gi.element_batch,gc.element_batch) as element_batch,
    nvl(gi.session_common_impression, 0) as session_common_impression,
    nvl(gi.session_common_click,0) as session_common_click,
    nvl(gi.views,0) as views,
    nvl(gc.cart,0) as cart
from tmp_table_two gi
full join goods_cart gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.platform=gc.platform and gi.country=gc.country
and gi.element_batch=gc.element_batch and gi.element_tag=gc.element_tag
),

tmp_table_four as (
select
/*+ REPARTITION(10) */
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform,gc.platform) as platform,
    nvl(gi.country,gc.country) as country,
    nvl(gi.element_tag,gc.element_tag) as element_tag,
    nvl(gi.element_batch,gc.element_batch) as element_batch,
    nvl(gi.session_common_impression, 0) as session_common_impression,
    nvl(gi.session_common_click,0) as session_common_click,
    nvl(gi.views,0) as views,
    nvl(gi.cart,0) as cart,
    nvl(gc.video_impression,0) as video_impression
from tmp_table_three gi
full join goods_video_impression gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.platform=gc.platform and gi.country=gc.country
and gi.element_batch=gc.element_batch and gi.element_tag=gc.element_tag
)

insert overwrite table ads.ads_fd_goods_element_test
select
/*+ REPARTITION(10) */
goods_id,
project,
platform,
country,
element_tag,
element_batch,
sum(session_common_impression) as session_common_impression,
sum(session_common_click) as session_common_click,
case when nvl(sum(session_common_impression), 0)=0 then 0
else nvl(sum(session_common_click),0)/nvl(sum(session_common_impression), 0) end as session_common_ctr,
sum(views) as views,
sum(cart) as cart,
sum(video_impression) as video_impression,
sum(video_play) as video_play
from
(
select
/*+ REPARTITION(10) */
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform,gc.platform) as platform,
    nvl(gi.country,gc.country) as country,
    nvl(gi.element_tag,gc.element_tag) as element_tag,
    nvl(gi.element_batch,gc.element_batch) as element_batch,
    nvl(gi.session_common_impression, 0) as session_common_impression,
    nvl(gi.session_common_click,0) as session_common_click,
    nvl(gi.views,0) as views,
    nvl(gi.cart,0) as cart,
    nvl(gi.video_impression,0) as video_impression,
    nvl(gc.video_play,0) as video_play
from tmp_table_four gi
full join goods_video_play gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.platform=gc.platform and gi.country=gc.country
and gi.element_batch=gc.element_batch and gi.element_tag=gc.element_tag

union all

select
/*+ REPARTITION(10) */
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform,gc.platform) as platform,
    nvl(gi.country,gc.country) as country,
    nvl(gi.picture_group,gc.picture_group) as element_tag,
    nvl(gi.picture_batch,gc.picture_batch) as element_batch,
    nvl(gi.impression_session, 0) as session_common_impression,
    nvl(gc.click_session,0) as session_common_click,
    0 as views,
    0 as cart,
    0 as video_impression,
    0 as video_play
from goods_impression_picture gi
full join goods_click_picture gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.platform=gc.platform and gi.country=gc.country
and gi.picture_group=gc.picture_group and gi.picture_batch=gc.picture_batch
)
group by
goods_id,project,platform,country,element_tag,element_batch
;
