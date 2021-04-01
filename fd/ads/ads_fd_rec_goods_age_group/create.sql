create table goods_age_group
(
	goods_id int not null,
	age_group smallint null comment '1 未知，2 年轻，3 年老',
	ctime          timestamp      default CURRENT_TIMESTAMP not null comment '创建时间类型字段',
    mtime          timestamp      default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP comment '更新时间类型字段',
	constraint goods_age_group_pk
		primary key (goods_id)
);