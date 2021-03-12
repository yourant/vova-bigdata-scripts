create table goods_age_group
(
	goods_id int not null,
	age_group smallint null comment '0 年轻，1 年老，2 未知',
	ctime          timestamp      default CURRENT_TIMESTAMP not null comment '创建时间类型字段',
    mtime          timestamp      default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP comment '更新时间类型字段',
	constraint goods_age_group_pk
		primary key (goods_id)
);