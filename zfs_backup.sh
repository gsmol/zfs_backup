#!/usr/local/bin/bash -x

function comparison() {

for i in $(ssh $user@$host -p $port -o StrictHostKeyChecking=no zfs list -d 1 -r -H -o name -t snapshot $remote_set); ## array of remote snapshots
do
a+=($i)
done

for i in $(zfs list -d 1 -r -H -o name -t snapshot  $farm/$local_set/$remote_set); ## array of local snapshots
do
b+=($i)
done


for volumeA in "${a[@]}"
do
  for volumeB in "${b[@]}"
  do
    if [[ "$(echo $volumeA | sed 's/.*@//')" == "$(echo $volumeB | sed 's/.*@//')" ]] 
    then
    c+=( "$volumeA" )
    break
    fi
  done
done
}

function incremental() {
#Making new snapshot on remote
ssh $user@$host -p $port -o StrictHostKeyChecking=no zfs snapshot -r $remote_set@$NOW > /dev/null 2>&1

#Sending them incremental
ssh $user@$host -p $port -o StrictHostKeyChecking=no zfs send -R -i $last_snapshot $remote_set@$NOW | zfs recv -e -u $farm/$local_set > /dev/null 2>&1
}

function full() {
#Making new snapshot on remote
ssh $user@$host -p $port -o StrictHostKeyChecking=no zfs snapshot -r $remote_set@$NOW > /dev/null 2>&1
#destoy local set because it can be incompatible with remote
zfs destroy -r -f $farm/$local_set/$remote_set > /dev/null 2>&1
#create local set if it`s missing
zfs create -o mountpoint=none -o canmount=off $farm/$local_set > /dev/null 2>&1
#Sending them in full stream
ssh $user@$host -p $port -o StrictHostKeyChecking=no zfs send -R $remote_set@$NOW | zfs recv -F -e -u $farm/$local_set > /dev/null 2>&1
}

remote_set="$1"     # zroot or "zroot/something" or even "zroot/something/somewhere"
local_set="$2"      # dbs1/php1/ssd1
host="$3"
port="22"

if [[ ! -z $4 ]] 
then port="$4"
fi

user="zbackup"
farm="zdata/backup"
NOW=`date  +%Y.%m.%d-%H:%M:%S`
full="false"
lock="/tmp/zfs.lock"

while [[ -f $lock ]]
do sleep 15
done

touch $lock

#checking if there any snapshots at all on local dataset

zfs list -t snapshot -d 2 -o name -r -H $farm/$local_set/$remote_set | sort | grep $farm/$local_set/$remote_set > /dev/null 2>&1
if [[ $? -ne 0 ]]
then 
full="true" #there is no snapshots on local dataset. Its going to be a full backup
fi

#1. Check snapshots existence on remote

if [[ $full == "false" ]]
then
ssh $user@$host -p $port -o StrictHostKeyChecking=no zfs list -t snapshot -o name | sort | grep $remote_set > /dev/null 2>&1
  if [[ $? -ne 0 ]]
  then 
  full="true" #there is no snapshots on remote dataset. Its going to be a full backup
  fi
fi


#2 Compare datasets numbers. If they are not even, then between last  and current backups some datasets has been removed or added. 

if [[ $full == "false" ]] 
then
list_remote=$(ssh $user@$host -p $port -o StrictHostKeyChecking=no zfs list -r -H -o name -t filesystem $remote_set | wc -l | awk '{gsub(/^ +| +$/,"")} {print $0 }')
list_local=$(zfs list -r -H -o name -t filesystem $farm/$local_set/$remote_set | wc -l | awk '{gsub(/^ +| +$/,"")} {print $0 }')
  if [[ $list_remote -ne $list_local ]]
  then
  full="true"
  fi
fi

#3. Comparing snapshots on remote and local

if [[ $full == "false" ]] 
then
comparison #создает массивы 'а' и 'b', сравнивает и отдает массив с, содержающий совпадения 'а' с 'b'
  if [[ ${#c[@]} -ne 0 ]]
  then
#there is a match. Picking a last snapshot
  last_snapshot="${c[${#c[*]}-1]}"
#create snapshots on remote machine and incrementally fetch them
  incremental
    if [[ $? -eq 0 ]] 
    then 
#incremental backup successfully completed
    success="inc_true"
    else 
#incremental backup failed for some reason
    rm -f $lock 
    exit 1 
    fi		
  else
  #no match. its going to be a full backup
  full="true"
  fi
fi

#4. Fullbackup

if [[ $full == "true" ]] 
then
full #Creating snapshots on remote and sending in full stream
  if [[ $? -eq 0 ]]
  then 
  #fullbackup is successfull
  success="full_true"
  else 
  #full backup failed for some reason
  rm -f $lock
  exit 1
  fi
fi

#5. Delete old snapshots on remote and local

if [[ $success == "inc_true" ]]
then
  for volumeA in "${a[@]}"
  do ssh $user@$host -p $port -o StrictHostKeyChecking=no zfs destroy -r -f $volumeA
  done

  for volumeB in "${b[@]}"
  do zfs destroy -r -f $volumeB
  done
  rm -f $lock
  exit 0
elif [[ $success == "full_true" ]]
then
  rm -f $lock
  exit 0
else
  rm -f $lock
  exit 1
fi