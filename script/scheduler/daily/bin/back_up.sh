#! /bin/bash


arr=$2
OLD_IFS="$IFS"
IFS=","
sync_tables=(${arr})
IFS="$OLD_IFS"

etl_date=$1
w_path="bin"
tmp_file=${w_path}/tmp_bak_hbase.sql
cur_namespace=ecifdb${etl_date}


rm -f ${tmp_file}

list_snapshots=`hbase shell <<EOF
list_snapshots
EOF
`

sql=`
    for table in ${sync_tables[@]};do
        [[ ${list_snapshots} =~ "tableSnapshot_${cur_namespace}_${table}" ]] && echo "delete_snapshot 'tableSnapshot_${cur_namespace}_${table}'" >> ${tmp_file}
        echo "disable '${cur_namespace}:${table}'" >> ${tmp_file}
        echo "snapshot '${cur_namespace}:${table}', 'tableSnapshot_${cur_namespace}_${table}'" >> ${tmp_file}
        echo "enable '${cur_namespace}:${table}'" >> ${tmp_file}
        echo "" >> ${tmp_file}
    done

    echo "exit" >> ${tmp_file}
`

sql=`cat ${tmp_file}`
hbase shell <<EOF
    ${sql}
EOF

