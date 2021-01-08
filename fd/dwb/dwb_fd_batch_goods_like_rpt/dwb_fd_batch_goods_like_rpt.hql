
insert overwrite table dwb.dwb_fd_batch_goods_like_rpt
SELECT
       batch,
       virtual_goods_id,
       project,
       nvl(country, "ALL"),
       nvl(platform_type, "ALL"),
       sum(like_num),
       sum(unlike_num),
       sum(impressions)
from
(
    select
         batch,
         virtual_goods_id,
         project,
         country,
         platform_type,
        count(distinct if(event = "goods_like", session_id, null))       as like_num,
        count(distinct if(event = "goods_dislike", session_id, null))    as unlike_num,
        count(distinct if(event = "goods_impression", session_id, null)) as impressions
    from dwd.dwd_fd_batch_detail
    where pt>=date_add('${pt}',-60)
    group by batch,virtual_goods_id,project,country,platform_type
) t1
 where t1.batch >= '${batchNum}'
group by batch,virtual_goods_id,project,country,platform_type
    grouping sets (
    ( batch, virtual_goods_id, project, country, platform_type),
    ( batch, virtual_goods_id, project, country),
    ( batch, virtual_goods_id, project, platform_type ),
    ( batch, virtual_goods_id, project)
    );