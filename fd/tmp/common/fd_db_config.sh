#!/bin/bash

: '
	数据库配置
'

#vbridal数据库配置信息【测试】
declare -A vbridal_db
vbridal_db=([db_host]="172.31.88.225" [db_port]="3306" [db_databases]="vbridal" [db_user_name]="datagrouptest" [db_password]="datagroupA123")

#vbridal数据库配置信息【正式】
#declare -A vbridal_db
#vbridal_db=([db_host]="slave5.fdrds.zone" [db_port]="3306" [db_databases]="vbridal" [db_user_name]="artemis" [db_password]="WiIbSqMc1IxT")

#artemis数据库配置信息【测试】
declare -A artemis_db
artemis_db=([db_host]="artemistest.cwkl0pzdp8cf.us-east-1.rds.amazonaws.com" [db_port]="3306" [db_databases]="artemis" [db_user_name]="artemistest" [db_password]="artemispwd")

#artemis数据库配置信息【正式】
#declare -A artemis_db
#artemis_db=([db_host]="artemis2slave.cpbbe5ehgjpf.us-east-1.rds.amazonaws.com" [db_port]="3306" [db_databases]="artemis" [db_user_name]="market" [db_password]="MyF4k2y9jJSv")


#erp ecshop数据库配置信息【正式】
#declare -A ecshop_db
#artemis_db=([db_host]="artemis2slave.cpbbe5ehgjpf.us-east-1.rds.amazonaws.com" [db_port]="3306" [db_databases]="ecshop-hk" [db_user_name]="market" [db_password]="MyF4k2y9jJSv")

#erp ecshop数据库配置信息【正式】
#declare -A romeo_db
#artemis_db=([db_host]="artemis2slave.cpbbe5ehgjpf.us-east-1.rds.amazonaws.com" [db_port]="3306" [db_databases]="romeo-hk" [db_user_name]="market" [db_password]="MyF4k2y9jJSv")
