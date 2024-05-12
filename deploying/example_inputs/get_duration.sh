for D in "$@"; do
 echo -n "PP $(cat "$D"/parallel*/parallel_example/trace*.csv | sort | tail -n1 | cut -d "," -f 1) " 
 echo -n "SP(S)P $(cat "$D"/stateparallel*/stateparallel_example/trace*.csv | sort | tail -n1 | cut -d "," -f 1) " 
 echo "($D)"
done                                                                            
