#! /bin/bash

table=$1
#table=similar_pty_cert
indexFields=$2
#indexFields=cust_id
namespace=ecifdb$3
#router.field
ROUTER_FIELD=$4
w_path="bin"
conf_path=${w_path}/../conf/${table}_${namespace}

n=`echo $1 | tr 'a-z' 'A-Z'`
hbase_name=${namespace}:${n}

if [ -d "${conf_path}" ]; then
    rm -fr ${conf_path}
fi

#.1 生成配置目录
solrctl instancedir --generate ${conf_path}

#.2修改 scheme.xml
OLD_IFS="$IFS" 
IFS="," 
arr=(${indexFields})
IFS="$OLD_IFS" 
for s in ${arr[@]} 
do 
    conf="  <field name=\"solr_$s\" type=\"string\" indexed=\"true\" stored=\"true\" />"
    sed -i -r "101a\ ${conf}" ${conf_path}/conf/schema.xml
    echo "$s" 
done
echo "修改 scheme.xml结束"

#.3 修改成硬提交模式
echo "正在修改成硬提交模式"
sed -i -r "401s/15000/1000/" ${conf_path}/conf/solrconfig.xml
#sed -i -r "392s/false/true/" ${conf_path}/conf/solrconfig.xml

echo "修改成硬提交模式结束"
#.4 创建实例
echo "正在创建实例"
solrctl instancedir --create dir_${table}_${namespace} ${conf_path} 
echo "创建实例结束"

#.5 创建collection
echo "正在创建collection"
m=`sh ${w_path}/chooseEnv.sh 6 15`
s=`sh ${w_path}/chooseEnv.sh 2 5`
r=`sh ${w_path}/chooseEnv.sh 3 3`
solrctl collection --create ${namespace}_${table} -c dir_${table}_${namespace} -m ${m} -s ${s} -r ${r} -a -ckv router.name=compositeId -ckv ${ROUTER_FIELD}
echo "创建collection结束"

#.6 生成morphline-hbase-mapper.xml
echo "正在生成morphline-hbase-mapper.xml"
echo "<?xml version=\"1.0\"?>
<indexer table=\"${hbase_name}\" mapper=\"com.ngdata.hbaseindexer.morphline.MorphlineResultToSolrMapper\">
    <param name=\"morphlineFile\" value=\"morphlines.conf\"/>
    <param name=\"morphlineId\" value=\"${table}_morphlineId\"/>
</indexer>

" >> ${conf_path}/morphline-hbase-mapper.xml


echo "生成morphline-hbase-mapper.xml结束"

# #.7 生成morphline.conf 配置文件 9
 echo "正在生成morphline.conf 配置文件"
 mor_path=${conf_path}/morphlines.conf
echo "SOLR_LOCATOR : {" >> ${mor_path}
echo "  zkHost : \"\$ZK_HOST\" " >> ${mor_path}
echo "}" >>  ${mor_path}
echo "" >>  ${mor_path}
echo "morphlines : [" >>  ${mor_path}
echo "]" >>  ${mor_path}

sed -r -i "5a\ }" ${mor_path}
sed -r -i "5a\ ]" ${mor_path}
sed -r -i "5a\   { logDebug { format : \"output record: {}\", args : [\"@{}\"] } }" ${mor_path}
sed -r -i "5a\ " ${mor_path}
sed -r -i "5a\ " ${mor_path}
sed -r -i "5a\   }" ${mor_path}
sed -r -i "5a\     }" ${mor_path}
sed -r -i "5a\       ]" ${mor_path}

flag=true
for field in ${arr[@]};do
  if [[ ${flag}=true ]]; then
    sed -r -i "5a\         }" ${mor_path}
    flag=false
  else
    sed -r -i "5a\         }," ${mor_path}
  fi
  sed -r -i "5a\           source : value" ${mor_path}
  sed -r -i "5a\           type : string" ${mor_path}
  sed -r -i "5a\           outputField : \"solr_${field}\"" ${mor_path}
  sed -r -i "5a\           inputColumn : \"f:${field}\"" ${mor_path}
  sed -r -i "5a\         {" ${mor_path}
done

sed -r -i "5a\       mappings : [" ${mor_path}
sed -r -i "5a\     extractHBaseCells {" ${mor_path}
sed -r -i "5a\   {" ${mor_path}
sed -r -i "5a\ commands : [" ${mor_path}
sed -r -i "5a\ " ${mor_path}
sed -r -i "5a\ importCommands : [\"org.kitesdk.**\", \"com.ngdata.**\"]" ${mor_path}
sed -r -i "5a\ id : ${table}_morphlineId" ${mor_path}
sed -r -i "5a\    {" ${mor_path}

cat ${mor_path}
# #mor_path=`find /opt/cm-5.14.2/run/cloudera-scm-agent/process/ -name "morphlines.conf" | head -1`
# mor_path=${w_path}/morphlines.conf
# echo "" > $mor_path
# #is=`cat $mor_path | grep ${table}_morphlineId`
# #if [[ $is == "" ]];then
# sed -r -i "1a\ }," ${mor_path}
# sed -r -i "1a\ ]" ${mor_path}
# sed -r -i "1a\   { logDebug { format : \"output record: {}\", args : [\"@{}\"] } }" ${mor_path}
# sed -r -i "1a\ " ${mor_path}
# sed -r -i "1a\ " ${mor_path}
# sed -r -i "1a\   }" ${mor_path}
# sed -r -i "1a\     }" ${mor_path}
# sed -r -i "1a\       ]" ${mor_path}

# flag=true
# for field in ${arr[@]};do
#   if [[ ${flag}=true ]]; then
#     sed -r -i "1a\         }" ${mor_path}
#     flag=false
#   else
#     sed -r -i "1a\         }," ${mor_path}
#   fi
#   sed -r -i "1a\           source : value" ${mor_path}
#   sed -r -i "1a\           type : string" ${mor_path}
#   sed -r -i "1a\           outputField : \"solr_${field}\"" ${mor_path}
#   sed -r -i "1a\           inputColumn : \"f:${field}\"" ${mor_path}
#   sed -r -i "1a\         {" ${mor_path}
# done

# sed -r -i "1a\       mappings : [" ${mor_path}
# sed -r -i "1a\     extractHBaseCells {" ${mor_path}
# sed -r -i "1a\   {" ${mor_path}
# sed -r -i "1a\ commands : [" ${mor_path}
# sed -r -i "1a\ " ${mor_path}
# sed -r -i "1a\ importCommands : [\"org.kitesdk.**\", \"com.ngdata.**\"]" ${mor_path}
# sed -r -i "1a\ id : ${table}_morphlineId" ${mor_path}
# sed -r -i "1a\    {" ${mor_path}

# #else
# # echo "id已经存在"
# #fi

# echo "生成morphline.conf 配置文件结束"

#.8 创建索引
#echo "正在创建索引"
#hbase-indexer add-indexer \
#    --name ${namespace}_${table}_indexer \
#    --indexer-conf ${conf_path}/morphline-hbase-mapper.xml \
#    --connection-param solr.zk=ecif-cdh-prod-node3:2181/solr \
#    --connection-param solr.collection=${namespace}_${table} \
#    --zookeeper ecif-cdh-prod-node1:2181,ecif-cdh-prod-node12:2181,ecif-cdh-prod-node13:2181,ecif-cdh-prod-node2:2181,ecif-cdh-prod-node3:2181


#echo "创建索引结束"
#.9 同步数据
#echo "正在同步数据"

#sh ./sync.sh ${conf_path} ${table} ${mor_path}
#echo "手动重启同步"
#cat $mor_path

#hadoop --config /etc/hadoop/conf \
#    jar /opt/cloudera/parcels/CDH-5.14.2-1.cdh5.14.2.p0.3/jars/hbase-indexer-mr-1.5-cdh5.14.2-job.jar \
#    --conf /etc/hbase/conf.cloudera.hbase/hbase-site.xml \
#    --hbase-indexer-file ${conf_path}/morphline-hbase-mapper.xml \
#    --zk-host hadoop1:2181/solr \
#    --collection ${table} \
#    --morphline-file ${mor_path} \
#    --reducers 0 \
#    --go-live

#echo "同步数据结束"
