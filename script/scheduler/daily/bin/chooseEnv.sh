#! /bin/bash 


test_env=$1
pro_env=$2

is_pro=true
env=`cat /apps/environment`
if [[ ${env} =~ "test" ]];then
    is_pro=false
fi

if ${is_pro}; then
    echo ${pro_env}
else
    echo ${test_env}
fi