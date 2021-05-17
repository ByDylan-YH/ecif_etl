#! /bin/bash
etl_date=$1
if [[ ${etl_date} -eq '' ]]; then
        echo "请输入etl_date!"
        exit
fi
#工作目录
w_path="bin"
{
    echo "同步namespace" &&
    bash ${w_path}/run.sh 0 9999 ${etl_date}
}&&{
    echo "MISSION SUCCESS!"
}||{
    echo "MISSION FAILED!"
    exit 1
}