#!/bin/bash

echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

echo "----------开始insert-------"
# 逻辑sql
sql="
use themis;
-- 更新红包(实时)表中已有的数据
create temporary table tmp(
  goods_id int(11) UNIQUE key,
  is_invalid  tinyint(1),
  red_packet_cnt int(9),
  is_delete  tinyint(1),
  recommend_goods_id int(11)
);

insert into tmp (
  goods_id,
  is_invalid,
  red_packet_cnt,
  is_delete,
  recommend_goods_id
)
select
  t2.goods_id goods_id,
  0,
  0,
  0,
  t1.recommend_goods_id recommend_goods_id
from
(
  select * from
  (
    select * from
    (
      select distinct
        recommend_goods_id recommend_goods_id,
        red_packet_cnt red_packet_cnt,
        goods_sn goods_sn
      from
      (
        select distinct
          recommend_goods_id recommend_goods_id,
          red_packet_cnt red_packet_cnt
        from
          ads_lower_price_goods_red_packet
        where is_invalid = 0 and is_delete = 0 and red_packet_cnt > 0 and goods_id = recommend_goods_id
      ) t1
      left join
      ads_vova_red_packet_gsn_goods t2
      on t1.recommend_goods_id = t2.goods_id
      where t2.goods_id is not null
    ) t1
    order by red_packet_cnt desc limit 1000000
  ) as t
  group by t.goods_sn
) t1
left join
( -- 红包结果表中没有的商品
  select
    distinct
    t1.goods_id goods_id,
    t1.goods_sn goods_sn
  from
    ads_vova_red_packet_gsn_goods t1
  left join
  (
    select *
    from
      ads_lower_price_goods_red_packet
    where is_delete = 0
  ) t2
  on t1.goods_id = t2.goods_id
  where t2.goods_id is null
) t2
on t1.goods_sn = t2.goods_sn and t1.recommend_goods_id != t2.goods_id
where t1.recommend_goods_id != t2.goods_id
  and t2.goods_id is not null
;

-- 重复数据需处理
create temporary table tmp1 (
  goods_id int(11) UNIQUE key
);

insert into tmp1 (
select
  t1.goods_id as goods_id
from
  tmp t1
left join
  ads_lower_price_goods_red_packet t2
on t1.goods_id = t2.goods_id
where t2.goods_id is not null
and t2.red_packet_cnt > 0
)
;

delete from tmp where goods_id in (select goods_id from tmp1);

create temporary table tmp2 (
  goods_id int(11) UNIQUE key
);

insert into tmp2 (
select
  t2.goods_id
from
  tmp t1
left join
  ads_lower_price_goods_red_packet t2
on t1.goods_id = t2.goods_id
where t2.goods_id is not null
  and t2.red_packet_cnt = 0
)
;

delete from ads_lower_price_goods_red_packet where goods_id in (select goods_id from tmp2);

-- 插入红包(实时)表中没有的数据
insert into ads_lower_price_goods_red_packet
(
  goods_id,
  is_invalid,
  red_packet_cnt,
  is_delete,
  recommend_goods_id
) select
  goods_id,
  is_invalid,
  red_packet_cnt,
  is_delete,
  recommend_goods_id
from tmp
;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
