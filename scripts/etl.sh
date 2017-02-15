setup=$1
dataset=$2
HOSTLIST=`cat hosts`
i=0
for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  ssh $SUCCINCT_SSH_OPTS "$host" "~/etl-local.sh" $setup $dataset $i 2>&1 | sed "s/^/$host: /" &
  i=$(( $i + 1 ))
done
wait
