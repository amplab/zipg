agg_pid=`pgrep graph_query_agg`
gdb attach $agg_pid -ex cont
