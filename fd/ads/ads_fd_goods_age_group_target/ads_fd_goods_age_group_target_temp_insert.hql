SET hive.exec.compress.output=true;

with users_birthday as (
select
ofu.user_id,
ofud.sp_duid,
ofu.birthday,
ofu.reg_site_name,
if(ofu.birthday is null or ofu.birthday in ('1900-01-01','1990-01-01','1970-01-01','0000-00-00') or substr(ofu.birthday,6,5) in ('07-01', '01-01'),0,months_between(current_date, ofu.birthday) div 12) as age
from ods_fd_vb.ods_fd_users ofu
left join ods_fd_vb.ods_fd_user_duid ofud
on ofu.user_id = ofud.user_id
),

dwb_fd_goods_target as (
select
    virtual_goods_id as goods_id,
    project,
    country,
    if(platform_type='mobile_web', 'H5',if(platform_type in ('android_app', 'ios_app'),'app',if(platform_type in ('tablet_web','pc_web'),'PC','other'))) as platform_type,
    record_type,
    list_type,
    user_id,
    domain_userid,
    goods_uv
from
dwb.dwb_fd_goods_snowplow_target
where pt >= '${pt_begin}' and pt <= '${pt_end}'
and list_type = 'list-category' and record_type in ('click','impression')
and goods_id is not null and length(country) < 3
)

insert overwrite table tmp.goods_user_birthday
select
/*+ REPARTITION(50) */
    dfgt.goods_id,
    dfgt.project,
    dfgt.country,
    dfgt.platform_type,
    dfgt.record_type,
    dfgt.list_type,
    dfgt.goods_uv,
    ub.age,
    if(ub.age = 0 or ub.age is null,0,
    if(ub.age > 0 and ub.age <= 17,1,
    if(ub.age > 17 and ub.age <= 24,2,
    if(ub.age > 24 and ub.age <= 34,3,
    if(ub.age > 34 and ub.age <= 44,4,
    if(ub.age > 44 and ub.age <= 54,5,
    if(ub.age > 54 and ub.age <= 64,6,7
    ))))))) as age_group
from
    dwb_fd_goods_target dfgt
left join users_birthday ub
on dfgt.domain_userid = ub.sp_duid
;
