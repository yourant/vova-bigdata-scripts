#!/bin/bash
#指定日期和引擎

if [ $? -ne 0 ];then
  exit 1
fi

freedoms="$1"

if [ ! -n "$freedoms" ]; then
  exit 1
fi

dt=$(echo $freedoms | jq '.dt'| sed $'s/\"//g')
model_type=$(echo $freedoms | jq '.model_type'| sed $'s/\"//g')
table_name=$(echo $freedoms | jq '.table_name'| sed $'s/\"//g')

echo "start to export model:$model_type,table:$table_name with dt:$dt!"
if [ ! -n "$dt" ]; then
  exit 1
fi

if [ ! -n "$model_type" ]; then
  exit 1
fi

if [ ! -n "$table_name" ]; then
  exit 1
fi
select_sql="select  tab_name from  als_images.mlb_vova_goods_emb_relation where link_model='mlb_vova_search_goods_emb' and use_online=0 limit 1"
db_data=`mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${select_sql}"`
suffix=`echo ${db_data: -1}`

if [ ! -n "$suffix" ]; then
  exit 1
fi

tables=(${table_name//,/ })
for var in "${tables[@]}"
do
  if [[ "$var" =~ ^mlb_vova_search_goods_emb.* ]]
  then
    echo "sh /mnt/vova-bigdata-scripts/vova/mlb/goods_emb_search/sqoop_export_goods_emb.sh ${var} ${dt}  ${suffix}"
    sh /mnt/vova-bigdata-scripts/vova/mlb/goods_emb_search/sqoop_export_goods_emb.sh "${var}" "${dt}"  "${suffix}"
    if [ $? -ne 0 ];then
       exit 1
    fi
  elif [[ "$var" =~ ^mlb_vova_recall_words_search.* ]]
  then
    echo "sh /mnt/vova-bigdata-scripts/vova/mlb/goods_emb_search/sqoop_export_recall_words_search.sh ${var} ${dt}  ${suffix}"
    sh /mnt/vova-bigdata-scripts/vova/mlb/goods_emb_search/sqoop_export_recall_words_search.sh "${var}" "${dt}"  "${suffix}"
    if [ $? -ne 0 ];then
       exit 1
    fi
  fi
done

if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/vova/mlb/goods_emb_search/send_msg.sh  "${suffix}"

if [ $? -ne 0 ];then
  exit 1
fi
