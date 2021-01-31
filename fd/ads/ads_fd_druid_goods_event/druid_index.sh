#bin/sh

ts=$(date -d "$1" +"%Y-%m-%d %H")

if [ "$#" -ne 3 ]; then
  pt=$(date -d "$ts -1 hours" +"%Y-%m-%d")
  hour=$(date -d "$ts -1 hours" +"%H")
  start=$(date -d "$ts -1 hours" +"%Y-%m-%dT%H:00:00.000Z")
  end=$(date -d "$ts" +"%Y-%m-%dT%H:00:00.000Z")
else
  pt=$2
  hour=$3
  start=$(date -d "${pt} ${hour} -1 hours" +"%Y-%m-%dT%H:00:00.000Z")
  end=$(date -d "${pt} ${hour}" +"%Y-%m-%dT%H:00:00.000Z")
fi

echo "pt: ${pt}"
echo "hour: ${hour}"
echo "start:${start}"
echo "end:${start}"

echo "===="
echo "{\"type\":\"index_parallel\",\"spec\":{\"dataSchema\":{\"dataSource\":\"goods_test1\",\"parser\":{\"parseSpec\":{\"format\":\"json\",\"timestampSpec\":{\"format\":\"auto\",\"column\":\"event_time\"},\"dimensionsSpec\":{\"dimensions\":[\"record_type\",\"project\",\"user_id\",\"cat_id\",\"goods_id\",\"domain_userid\",\"session_id\",\"platform_type\",\"country\",\"virtual_goods_id\",\"page_code\",\"list_type\",\"absolute_position: Long\",\"url_route_sn\"]}}},\"metricsSpec\":[{\"type\":\"longSum\",\"name\":\"event_num\",\"fieldName\":\"event_num\",\"expression\":null},{\"type\":\"longSum\",\"name\":\"order_num\",\"fieldName\":\"order_num\",\"expression\":null},{\"type\":\"longSum\",\"name\":\"paying_order_num\",\"fieldName\":\"paying_order_num\",\"expression\":null},{\"type\":\"longSum\",\"name\":\"paid_order_num\",\"fieldName\":\"paid_order_num\",\"expression\":null}],\"granularitySpec\":{\"type\":\"uniform\",\"segmentGranularity\":\"HOUR\",\"queryGranularity\":\"HOUR\",\"rollup\":true,\"intervals\":[\"${start}/${end}\"]},\"transformSpec\":{\"filter\":null,\"transforms\":[]}},\"ioConfig\":{\"type\":\"index_parallel\",\"firehose\":{\"type\":\"static-s3\",\"uris\":[],\"prefixes\":[\"s3://bigdata-offline/warehouse/ads/ads_fd_druid_goods_event/pt=${pt}/hour=${hour}/\"],\"maxCacheCapacityBytes\":1073741824,\"maxFetchCapacityBytes\":1073741824,\"prefetchTriggerBytes\":536870912,\"fetchTimeout\":60000,\"maxFetchRetry\":3},\"appendToExisting\":true},\"tuningConfig\":{\"type\":\"index_parallel\",\"maxRowsPerSegment\":null,\"maxRowsInMemory\":1000000,\"maxBytesInMemory\":0,\"maxTotalRows\":null,\"numShards\":null,\"partitionsSpec\":null,\"indexSpec\":{\"bitmap\":{\"type\":\"concise\"},\"dimensionCompression\":\"lz4\",\"metricCompression\":\"lz4\",\"longEncoding\":\"longs\"},\"indexSpecForIntermediatePersists\":{\"bitmap\":{\"type\":\"concise\"},\"dimensionCompression\":\"lz4\",\"metricCompression\":\"lz4\",\"longEncoding\":\"longs\"},\"maxPendingPersists\":0,\"forceGuaranteedRollup\":false,\"reportParseExceptions\":false,\"pushTimeout\":0,\"segmentWriteOutMediumFactory\":null,\"maxNumConcurrentSubTasks\":5,\"maxRetry\":3,\"taskStatusCheckPeriodMs\":1000,\"chatHandlerTimeout\":\"PT10S\",\"chatHandlerNumRetries\":1,\"maxNumSegmentsToMerge\":100,\"totalNumMergeTasks\":10,\"logParseExceptions\":false,\"maxParseExceptions\":2147483647,\"maxSavedParseExceptions\":0,\"buildV9Directly\":true,\"partitionDimensions\":[]}}}"