
sql="

select
u.reg_site_name,
ud.sp_duid domain_userid
from ods_fd_vb.ods_fd_user_duid ud
join ods_fd_vb.ods_fd_users u on ud.user_id = u.user_id
where u.reg_site_name in ('floryday','airydress')
group by u.reg_site_name,ud.sp_duid

select
project,
count(*),
count(distinct domain_userid)
from
(
select
project,
domain_userid
from ods_fd_snowplow.ods_fd_snowplow_view_event where pt>='2021-05-07' and pt<='2021-05-13'
and project in ('floryday','airydress') and  domain_userid is not null and domain_userid!='' and user_id>0
group by project,domain_userid
) t group by project;

airydress	4826175
floryday	4545685

airydress	4826175	4826175
floryday	4545685	4545685

airydress	4681705
floryday	4420477

airydress	44417
floryday	59254
=================
airydress	39617
floryday	52849


select
project,
count(*),
count(distinct domain_userid)
from
(
select
t.project,
t.domain_userid
from
(
select
project,
domain_userid
from ods_fd_snowplow.ods_fd_snowplow_view_event where pt>='2021-05-07' and pt<='2021-05-13'
and project in ('floryday','airydress') and  domain_userid is not null and domain_userid!=''  and user_id>0
group by project,domain_userid
) t inner join
(
select
u.reg_site_name,
ud.sp_duid
from ods_fd_vb.ods_fd_user_duid ud
join ods_fd_vb.ods_fd_users u on ud.user_id = u.user_id
where u.reg_site_name in ('floryday','airydress') and  sp_duid is not null and sp_duid!=''
group by u.reg_site_name,ud.sp_duid
) t1 on t.project = t1.reg_site_name and t.domain_userid = t1.sp_duid
) t group by project




select
t.project,
t.domain_userid
from
(
select
project,
domain_userid
from ods_fd_snowplow.ods_fd_snowplow_view_event where pt>='2021-05-07' and pt<='2021-05-13'
and project in ('floryday','airydress') and  domain_userid is not null and domain_userid!=''  and user_id>0
group by project,domain_userid
) t left  join
(
select
u.reg_site_name,
ud.sp_duid
from ods_fd_vb.ods_fd_user_duid ud
join ods_fd_vb.ods_fd_users u on ud.user_id = u.user_id
where u.reg_site_name in ('floryday','airydress') and  sp_duid is not null and sp_duid!=''
group by u.reg_site_name,ud.sp_duid
) t1 on t.project = t1.reg_site_name and t.domain_userid = t1.sp_duid
where t1.reg_site_name is null and t1.sp_duid is null

"