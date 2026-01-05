#!/bin/bash
v_JobName="STREAMING_XXX_XXX_XXX_LMO"
v_QueueName="root.grp_xxx_xxx_cmp"
v_app_id=$(yarn application --list | grep -w "${v_JobName}.*${v_QueueName}"|awk '{print $1}')
if [ ! -z  "$v_app_id" ]; then
echo "WARNING:The ${v_JobName} ( ${v_app_id} ) is already running  state, so please stop it before restart."
exit;
else
sleep 20
v_app_id=$(yarn application --list | grep -w "${v_JobName}.*${v_QueueName}"|awk '{print $1}')
if [ ! -z  "$v_app_id" ]; then
echo "WARNING:The ${v_JobName} ( ${v_app_id} ) is already running  state, so please stop it before restart."
exit;
fi
fi
basePath=/hadoopfs/fs1/data0/sparkstreaming/xxx
streamingLog=${basePath}/loyalty_member_profile/lmo/scripts/streaming_xxx_xxx_xxx_lmo.log
jar_name=edh-xxx-xxx-kafkaToLmo-stream-1.0.0-shaded.jar
[ -e ${streamingLog} ] && rm ${streamingLog}
echo jar ${jar_name}
echo ---------- starting -------------------
echo
keytab=${basePath}/xxx_xxx_xxx/lmo/conf/dev-env02-xxx.keytab
keytab1=${basePath}/xxx_xxx_xxx/lmo/conf/xxx.keytab
[ -e ${keytab} ] && rm ${keytab} && rm ${keytab1}
hdfs dfs -copyToLocal hdfs:///user/xxx/loyalty/security/dev-env02-xxx.keytab ${basePath}/xxx_xxx_xxx/lmo/conf/.
mv ${basePath}/xxx_xxx_xxx/lmo/conf/dev-env02-xxx.keytab ${basePath}/xxx_xxx_xxx/lmo/conf/xxx.keytab
hdfs dfs -copyToLocal hdfs:///user/xxx/xxx/security/dev-env02-xxx.keytab ${basePath}/xxx_xxx_xxx/lmo/conf/.

echo ----------- kerberos initialized ------------------
echo
jarname=${basePath}/xxx_xxx_xxx/lmo/jars/${jar_name}
[ -e ${jarname} ] && rm ${jarname}
echo ----------- deleted jar file in local location ------------------
echo
echo started copying latest jar from the hdfs location
hdfs dfs -copyToLocal hdfs:////user/xxx/xxx/conf/streaming/xxx_xxx_xxx_lmo/${jar_name} ${basePath}/xxx_xxx_xxx/lmo/jars/.
echo ---------- copied jar to local -------------------
echo
echo executing spark submit command
export HADOOP_CONF_DIR=/etc/hadoop/conf:/etc/hive/conf:/opt/cod--1f1862gu2fmxx/hbase-conf
export SPARK_CLASSPATH=/etc/hadoop/conf:/etc/hive/conf:/opt/cod--1f1862gu2fmxx/hbase-conf

nohup /opt/cloudera/parcels/CDH/bin/spark3-submit --master yarn --deploy-mode cluster --jars /hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/jars/delta-core_2.12-2.4.0.jar,/hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/jars/delta-storage-2.4.0.jar,/opt/cloudera/parcels/CDH/jars/kafka_2.12-3.4.1.7.2.18.200-39.jar --files /hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/conf/jaas.conf,/hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/conf/application.conf,/hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/conf/dev-env02-xxx.keytab,/hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/conf/log4j.properties --conf "spark.yarn.submit.waitAppCompletion=false" --conf 'spark.executor.memory=13G' --conf 'spark.driver.memory=8G' --conf 'spark.executor.cores=4' --conf 'spark.executor.instances=11' --conf spark.yarn.keytab=/hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/conf/xxx.keytab --conf spark.yarn.principal=xxx@dev-ENV0.G16Z-ZG1G.CLOUDERA.SITE --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf -Dlog4j.configuration=log4j.properties" --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf -Dlog4j.configuration=log4j.properties" --conf "spark.eventLog.dir=hdfs:///user/spark/spark3ApplicationHistory" --conf "spark.eventLog.enabled=true" --conf "spark.yarn.historyServer.address=http://mneu-p-a-devstrm02-master0.dev-env0.g16z-zg1g.cloudera.site:18489"  --conf "spark.app.name=STREAMING_xxx_xxx_xxx_LMO" --conf 'spark.dynamicAllocation.enabled=false' --conf "spark.sql.parquet.compression.codec=zstd" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" --class com.emirates.edh.driver.xxxKafkaToLMODriver ${jarname} application.conf ${jarname} >> ${streamingLog} & 


#working
#nohup /opt/cloudera/parcels/CDH/bin/spark3-submit --master yarn --deploy-mode cluster --jars /hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/jars/delta-core_2.12-2.4.0.jar,/hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/jars/delta-storage-2.4.0.jar,/opt/cloudera/parcels/CDH/jars/kafka_2.12-3.4.1.7.2.18.200-39.jar --files /hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/conf/jaas.conf,/hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/conf/application.conf,/hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/conf/dev-env02-xxx.keytab,/hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/conf/log4j.properties --conf "spark.yarn.submit.waitAppCompletion=false" --conf 'spark.executor.memory=9G' --conf 'spark.driver.memory=6G' --conf 'spark.executor.cores=5' --conf 'spark.executor.instances=9' --conf spark.yarn.keytab=/hadoopfs/fs1/data0/sparkstreaming/xxx/xxx_xxx_xxx/lmo/conf/xxx.keytab --conf spark.yarn.principal=xxx@dev-ENV0.G16Z-ZG1G.CLOUDERA.SITE --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf -Dlog4j.configuration=log4j.properties" --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf -Dlog4j.configuration=log4j.properties" --conf "spark.eventLog.dir=hdfs:///user/spark/spark3ApplicationHistory" --conf "spark.eventLog.enabled=true" --conf "spark.yarn.historyServer.address=http://mneu-p-a-devstrm02-master0.dev-env0.g16z-zg1g.cloudera.site:18489"  --conf "spark.app.name=STREAMING_xxx_xxx_xxx_LMO" --conf 'spark.dynamicAllocation.enabled=false' --conf "spark.sql.parquet.compression.codec=zstd" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" --class com.emirates.edh.driver.xxxKafkaToLMODriver ${jarname} application.conf ${jarname} >> ${streamingLog} & 



echo ---------- spark job started in the background -------------------
echo
echo ---------- ending -------------------
