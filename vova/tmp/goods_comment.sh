sql="
select c.goods_id,  g.goods_name,  g.cat_id,  row_number() over(partition by c.goods_id order by c.post_time) id,  c.comment comment_text, c.rating comment_rating,  c.comment_has_img,  c.language_id,  l.comment_translation  from dwd.dwd_vova_fact_comment c  left join dim.dim_vova_goods g on g.goods_id = c.goods_id  left join (select comment_id,comment_translation from ods_vova_vts.ods_vova_goods_comment_languages where language_id =1) l on c.comment_id = l.comment_id  where c.datasource in ('vova','airyclub') order by c.goods_id,id


spark.sql("select c.goods_id,  g.goods_name,  g.cat_id,  row_number() over(partition by c.goods_id order by c.post_time) id,  c.comment comment_text, c.rating comment_rating,  c.comment_has_img,  c.language_id,  l.comment_translation  from dwd.dwd_vova_fact_comment c  left join dim.dim_vova_goods g on g.goods_id = c.goods_id  left join (select comment_id,comment_translation from ods_vova_vts.ods_vova_goods_comment_languages where language_id =1) l on c.comment_id = l.comment_id  where c.datasource in ('vova','airyclub') order by c.goods_id,id").repartition(8).write.option("header", "true").mode(SaveMode.Overwrite).json("s3://vova-chatbot/comment/plain/")



21417645

sh /mnt/vova-bigdata-scripts/common/sqoop_import_themis_2.sh --db_code=vts --table_name=goods_comment_languages  --etl_type=INIT  --period_type=day --partition_num=2



"