WITH user_info as (
    select sp_duid as duid,
           birthday
    from ods_fd_vb.ods_fd_user_duid ud
             inner join ods_fd_vb.ods_fd_users using (user_id)
),
     user_age AS (
         SELECT duid,
                cast(substr(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), 1, 4) - substr(birthday, 1, 4) as int) as age
         FROM user_info
         WHERE birthday != '1900-01-01'
           AND birthday != '1990-01-01'
           AND birthday != '1970-01-01'
           AND birthday != '1970-07-01'
           AND birthday IS NOT NULL
     ),-- 提取用户年龄,去除默认值和空值

     user_behavior AS (
         select domain_userid as duid,
                vg.goods_id   as goods_id
         from (
                  select domain_userid,
                         virtual_goods_id,
                         last_click_time,
                         row_number() over (partition by domain_userid order by last_click_time desc ) as click_index
                  from (
                           select domain_userid,
                                  goods_event_struct.virtual_goods_id as virtual_goods_id,
                                  max(derived_ts)                     as last_click_time
                           FROM ods_fd_snowplow.ods_fd_snowplow_goods_event
                           WHERE PT BETWEEN date_sub('${pt}', 30) AND date_sub('${pt}', 1)
                             and project = 'floryday'
                             and event_name = 'goods_click'
                           group by domain_userid, goods_event_struct.virtual_goods_id
                       ) user_click
              ) user_click_index
                  left join ods_fd_vb.ods_fd_virtual_goods vg using (virtual_goods_id)
         where click_index <= 20
           and goods_id is not null
     ),-- 提取最近30天用户最近20次的商品点击数据

     behavior_age_info AS (
         SELECT user_behavior.duid,
                user_behavior.goods_id,
                nvl(user_age.age, 51) as age
         FROM user_behavior
                  LEFT JOIN user_age
                            ON user_age.duid = user_behavior.duid
         WHERE user_age.age BETWEEN 10 AND 80
     ),-- 交互行为增加年龄标签并做一定的筛选

     goods_age AS (
         SELECT goods_id, AVG(age) AS goods_age_tag
         FROM behavior_age_info
         GROUP BY goods_id
     )-- 计算商品平均年龄，并对商品行为数少于5的做过滤，增加置信度
insert overwrite table ads.ads_fd_goods_age_group partition (pt='${pt}')
SELECT
/*+ REPARTITION(3) */
       goods_id,
       CASE
           WHEN goods_age_tag > 50 THEN 1
           WHEN goods_age_tag <= 50 THEN 0
           ELSE 0
           end AS goods_age_group
FROM goods_age;