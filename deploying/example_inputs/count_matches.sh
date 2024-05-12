for D in "$@"; do
 echo -n "PP $(cat "$D"/parallel*/parallel_example/0.log | grep LATE | wc -l) " 
 echo -n "SPP $(cat "$D"/stateparallel*/parallel_example/0.log | grep LATE | wc -l) "
 echo -n  "SPSP $(cat "$D"/stateparallel*/stateparallel_example/9.log | grep LATE | wc -l) "
 echo " ($D)"
done                                                                            
