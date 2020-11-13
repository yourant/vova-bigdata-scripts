CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_user_duid
(
    `id` bigint COMMENT '主键 自增id',
    `user_id` bigint COMMENT '',
    `sp_duid` string COMMENT '',
    `created_time` bigint COMMENT '',
    `last_update_time` bigint COMMENT ''
 )comment '用户id和打点id'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_user_duid
select id, user_id, sp_duid, created_time, last_update_time 
from (
	select 
		id,
		user_id,
		sp_duid,
		created_time,
		last_update_time,
		Row_Number() OVER (partition by user_id ORDER BY last_update_time desc) rank 
	from ods_fd_vb.ods_fd_user_duid_arc where dt = '${hiveconf:dt}'
)du where du.rank =1;
