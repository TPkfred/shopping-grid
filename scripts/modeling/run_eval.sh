

markets=(
 'EWR-FLL'
 'EWR-LAX'
 'EWR-LHR'
 'EWR-MIA'
 'JFK-LAX'
 'JFK-LGW'
 'JFK-LHR'
 'JFK-MIA'
 'LAX-EWR'
 'LAX-JFK'
 'LGA-LHR'
 'YYZ-LGA'
)


for market in "${markets[@]}"; do
    echo $market
    python eval_features.py -m $market --partition-window 1 13 --partition 
done