for dataset in "500000" "1000000" "2000000" "4000000"; do
  for q in `seq 0 1 49`; do
    bash sbin/setup.sh $dataset
    ./benchmark/bin/pbench $HOME/rpq/queries/${dataset}/query-${q}.succinct
  done
done
