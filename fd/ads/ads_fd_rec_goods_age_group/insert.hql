WITH user_age AS (
    SELECT domain_id as duid
         , age_group as age_label
    FROM ods_fd_rar.ods_fd_user_persona
    where age_group in (0, 1)
)-- 提取用户年龄标签,去除默认值和空值

   , user_behavior AS (
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
                        and project in ('floryday', 'airydress')
                        and event_name = 'goods_click'
                      group by domain_userid, goods_event_struct.virtual_goods_id
                  ) user_click
         ) user_click_index
             left join ods_fd_vb.ods_fd_virtual_goods vg using (virtual_goods_id)
    where click_index <= 20
      and goods_id is not null)-- 提取最近30天用户最近20次的商品点击数据

   , behavior_age_info AS (
    SELECT user_behavior.duid,
           user_behavior.goods_id,
           user_age.age_label
    FROM user_behavior
             INNER JOIN user_age ON user_age.duid = user_behavior.duid)-- 交互行为增加年龄标签

   , goods_age AS (
    SELECT goods_id, AVG(age_label) AS goods_age_tag
    FROM behavior_age_info
    GROUP BY goods_id
    HAVING COUNT(duid) >= 2)-- 计算商品平均年龄，并对商品行为数少于2的做过滤，增加置信度

   , goods_infer_age as (SELECT goods_id
                              , CASE
                                    WHEN goods_age_tag >= 0.5 THEN 1
                                    ELSE 0 END AS goods_age_group
                         FROM goods_age) --用户行为推算出来的商品年龄段
   , goods_on_sale as (
    select goods_id
    from ods_fd_vb.ods_fd_goods_project gp
    where lower(gp.project_name) IN ('floryday', 'airydress')
      AND gp.is_on_sale = 1
      AND gp.is_display = 1
      AND gp.is_delete = 0
    group by goods_id
)
   , goods_attr as (select g.goods_id,
                           a.attr_values as age_group
                    from goods_on_sale g
                             join ods_fd_vb.ods_fd_goods_attr ga ON ga.goods_id = g.goods_id AND ga.is_delete = 0
                             join ods_fd_vb.ods_fd_attribute a
                                  ON a.attr_id = ga.attr_id AND lower(a.attr_name) = 'age group' AND a.is_delete = 0
) --商品关联年龄属性
   , goods_age_attr_processed as (
    select goods_id,
           regexp_replace(age_group, "\\s+", "") as age_group,
           case
               when age_group regexp "(\\d+)\\s*\\+" then "age+"
               when age_group regexp "(\\d+)\\s*-\\s*(\\d+)" then "age-"
               else "unknow"
               end                               as attr_type,

           case
               when age_group regexp "(\\d+)\\s*\\+" then
                   if(regexp_extract(age_group, "(\\d+)\\s*\\+", 1) >= 35, 3, 1)
               when age_group regexp "(\\d+)\\s*-\\s*(\\d+)" then
                   case
                       when regexp_extract(age_group, "(\\d+)\\s*-\\s*(\\d+)", 1) >= 35 then 3
                       when regexp_extract(age_group, "(\\d+)\\s*-\\s*(\\d+)", 2) <= 35 then 2
                       else 1
                       end
               else 1
               end                               as age_type
    from goods_attr) --清洗年龄属性,算出年龄段
   , goods_attr_age as (
    select goods_id,
           case
               when array_contains(collect_set(age_type), 1) then 1
               when array_contains(collect_set(age_type), 2) and array_contains(collect_set(age_type), 3) then 1
               when array_contains(collect_set(age_type), 2) then 2
               when array_contains(collect_set(age_type), 3) then 3
               else 1
               end as goods_age_group
    from goods_age_attr_processed
    group by goods_id) --商品属性算出来的商品年龄段
insert overwrite table ads.ads_fd_goods_age_group partition (pt = '${pt}')
select goods_id,
       nvl(goods_infer_age.goods_age_group, goods_attr_age.goods_age_group) as goods_age_group,
       case
           when goods_infer_age.goods_age_group is not null then "user_action"
           when goods_attr_age.goods_age_group is not null then "goods_attr"
           else "unknow"
           end                                                              as source
from goods_attr_age
         full outer join goods_infer_age using (goods_id);
