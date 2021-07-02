SET hive.exec.compress.output=true;

with ads_fd_druid_goods_event_tmp as (
select
/*+ REPARTITION(20) */
    virtual_goods_id as goods_id,
    cat_id,
    country,
    project,
    session_id,
    record_type,
    list_type,
    if(platform_type='mobile_web', 'H5',if(platform_type in ('android_app', 'ios_app'),'app',if(platform_type in ('tablet_web','pc_web'),'PC','other'))) as platform_type,
    event_time,
    page_code,
    paying_order_num
from
    ads.ads_fd_goods_event
where pt >= '${pt_begin}' and pt <= '${pt_end}' and virtual_goods_id is not null and virtual_goods_id != ''
),

ads_fd_druid_goods_event_need as (
select * from ads_fd_druid_goods_event_tmp
where event_time >= '${time_begin}' and event_time <= '${time_end}'
),

only_testing_goods as
(select goods_id from ods_fd_vb.ods_fd_goods_test_goods where state >= 1 group by goods_id),

only_testing_virtual_goods as (
select vg.virtual_goods_id,vg.project_name from only_testing_goods tg
left join
ods_fd_vb.ods_fd_virtual_goods vg on vg.goods_id = tg.goods_id
)

insert overwrite table tmp.tmp_fd_druid_goods_event_temp
select
/*+ REPARTITION(10) */
    ge.goods_id,
    ge.cat_id,
    ge.country,
    ge.project,
    ge.session_id,
    ge.record_type,
    ge.list_type,
    ge.platform_type,
    ge.page_code,
    ge.paying_order_num,
    gr.virtual_goods_id
from ads_fd_druid_goods_event_need ge
inner join only_testing_virtual_goods gr on ge.goods_id = gr.virtual_goods_id and lower(ge.project) = lower(gr.project_name);