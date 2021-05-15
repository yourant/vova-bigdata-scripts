#!/bin/bash
#指定日期和引擎

suffix="$1"

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=mlb_vova_search_goods_emb_bg --from=data --to=service --jtype=1d --freedoms="{'model_name':'mlb_vova_search_goods_emb_b','suffix':'${suffix}'}"