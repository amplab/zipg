sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

CLIENTS="$sbin/zipg_clients"
SERVERS="$sbin/zipg_servers"
DATASET=$1
shift
NUMTHREADS=$1
shift

QOPTS=""
if [ "$#" = "0" ]; then
  query_type="fb"
elif [ "$#" != "10" ]; then
  echo "Must provide exactly 10 query opts: provided $#"
  exit 0
else
  for opt in $@; do
    perc=$(echo $opt | sed 's/\-D\(.*\)=\(.*\)/\2/')
    qtyp=$(echo $opt | sed 's/\-D\(.*\)=\(.*\)/\1/')
    if [ "$perc" = "100" ]; then
      query_type=$qtyp
    fi
    QOPTS="$QOPTS $opt"
  done
fi

echo "Query opts=\'$QOPTS\'"
echo "Query type=\'$query_type\'"

if [ -f "$CLIENTS" ]; then
  CLIENTLIST=`cat "$CLIENTS"`
else
  echo "CLIENTS file $CLIENTS does not exist."
  exit -1
fi

# By default disable strict CLIENT key checking
if [ "$SSH_OPTS" = "" ]; then
  SSH_OPTS="-o StrictHostKeyChecking=no"
fi

i=1
for CLIENT in `echo "$CLIENTLIST"|sed  "s/#.*$//;/^$/d"`; do
  SERVER=$(sed -n "$i{p;q;}" $SERVERS)
  echo "Launching client $CLIENT for server $SERVER..."
  ssh $SSH_OPTS "$CLIENT" $sbin/bench_zipg_node.sh $i $SERVER $DATASET $NUMTHREADS $QOPTS 2>&1 | sed "s/^/$CLIENT: /" &
  i=$(($i + 1))
done
wait

i=1
mkdir -p $sbin/../nt${NUMTHREADS}
for CLIENT in `echo "$CLIENTLIST"|sed  "s/#.*$//;/^$/d"`; do
  scp $SSH_OPTS "$CLIENT:$sbin/../zipg.t${NUMTHREADS}.q${query_type}.log" $sbin/../nt${NUMTHREADS}/zipg.n${i}.q${query_type}.log 2>&1 | sed "s/^/$CLIENT: /"
  ssh $SSH_OPTS "$CLIENT" rm $sbin/../zipg.t${NUMTHREADS}.q${query_type}.log 2>&1 | sed "s/^/$CLIENT: /"
  i=$(($i + 1))
done
