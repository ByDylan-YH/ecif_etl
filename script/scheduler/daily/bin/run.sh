#! /bin/bash

taskName=$1
taskType=$2
task=Run
etl_date=$3
w_path="bin"
run_mode=$4

executors=`sh ${w_path}/chooseEnv.sh 4 10`
cores=`sh ${w_path}/chooseEnv.sh 4 10`
executor_m=`sh ${w_path}/chooseEnv.sh 3G 8G`
driver_m=`sh ${w_path}/chooseEnv.sh 3G 8G`
jvm_opts="-XX:+UseG1GC -Djava.util.Arrays.useLegacyMergeSort=true"

spark-submit \
    --master yarn-client \
    --queue spark \
    --num-executors ${executors} \
    --executor-cores ${cores} \
    --executor-memory ${executor_m} \
    --driver-memory ${driver_m} \
    --conf spark.network.timeout=10000000 \
    --conf "spark.driver.extraJavaOptions=${jvm_opts}" \
    --conf "spark.executor.extraJavaOptions=${jvm_opts}" \
    --class com.hejin.etl.run.${task} ${w_path}/../lib/ecif-task-run-1.0.4-jar-with-dependencies.jar ${etl_date} ${taskName} ${taskType} ${run_mode}


