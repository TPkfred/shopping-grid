

# Lin Regr
overall MAPE: 15.0%



# Gradient Boosted Trees
GBT-Regr, one model for all markets:
overall MAPE: 14.4%
+-------+--------------------+
| market|                mape|
+-------+--------------------+
|LHR-EWR|0.046982340200654574|
|LHR-JFK| 0.05928787072118872|
|LHR-LAX| 0.09863349562329513|
|EWR-LHR| 0.09981652787013846|
|JFK-LHR| 0.10194048171014869|
|EWR-CDG| 0.10334854786042848|
|LAX-EWR| 0.11075375305733877|
|LAX-JFK| 0.12030932259329453|
|JFK-LAX| 0.12211097365267493|
|ATL-EWR| 0.14210145290178597|
|LGA-MIA|  0.2095614667697796|
|SFO-LAX| 0.21965693629901145|
|LAX-SFO| 0.22010284498942057|
|OAK-LAS| 0.43575656178084005|
+-------+--------------------+


# RF Regr
overall MAPE: 13.3%
+-------+--------------------+----------------------+
| market|mape-rf-single-model|mape-rf-individ-models|
+-------+--------------------+----------------------+
|LHR-EWR| 0.03601863988701263| 0.03575376455932613  |
|LHR-JFK|0.057528979278835914| 0.05396788600711592  |
|EWR-LHR|   0.064339470474955| 0.06054940473044798  |
|JFK-LHR| 0.09352950501069193| 0.08939990603942496  |
|LAX-EWR| 0.10154758151650022| 0.09487228006913749  |
|EWR-CDG| 0.11288659273284089|  0.1101875481510364  |
|LHR-LAX| 0.11337571493333565| 0.11641724229888734  |
|JFK-LAX| 0.11884334524102143|  0.1207931192983503  |
|LAX-JFK| 0.11982202249898405| 0.11531669610925546  |
|ATL-EWR| 0.12641200907072284|  0.1083288857335029  |
|LGA-MIA|  0.1952672733888646| 0.18094662295959915  |
|LAX-SFO|  0.2028225565392824| 0.19712604088975358  |
|SFO-LAX| 0.20561546207870865|  0.2071808811696239  |
|OAK-LAS|  0.4952043019033468| 0.45726291298851607  |
+-------+--------------------+----------------------+

feature importances:
('trailing_avg_fare', 0.3209345075359292)
('fr2_fare_prev_shop_day', 0.20100481104338677)
('fare_prev_shop_day', 0.16585044292420031)
('est_fare_from_next_dept_day', 0.08488915451793487)
('est_fare_from_prev_dept_day', 0.07502049700156743)
('fare_prev_dept_day', 0.04241239533988256)
('avg_fare_dtd', 0.03979723530861093)
('fare_next_dept_day', 0.030494199925413176)
('days_til_dept', 0.00811702169174535)
('trailing_avg_solution_counts', 0.006490518883809328)
('solution_counts_prev_shop_day', 0.005015096955370291)
('trailing_avg_shop_counts', 0.0045890973027149775)
('num_itin_prev_shop_day', 0.003851234028443653)
('shop_counts_prev_shop_day', 0.0035870646399046497)
('avg_out_avail_max_prev_shop_day', 0.002516591545239253)
('trailing_std_fare', 0.0021661553708295293)
('fr1_fr2_out_cxrs_overlap', 0.0012488322404018769)
('fr1_fr2_out_cxrs_same', 0.0010185833979618963)
('dept_dt_dow_int', 0.0009114447005432526)
('avg_out_avail_low_prev_shop_day', 5.1640211248415916e-05)
('is_holiday', 3.3475434862185555e-05)

## Excluding holiday dates:
Overall MAPE:  13.2%

+-------+-------------------+
| market|               mape|
+-------+-------------------+
|LHR-EWR|0.03872246779303075|
|LHR-JFK|0.05642332716873052|
|JFK-LHR|0.06878773573750313|
|EWR-LHR| 0.0925381468617531|
|LAX-EWR|0.09848388174706794|
|LAX-JFK|0.10551663265418912|
|JFK-LAX|0.10636445441698905|
|LHR-LAX|0.10795168470952722|
|ATL-EWR|0.12037048626742097|
|EWR-CDG|0.12553206585929064|
|LGA-MIA| 0.1843305877429315|
|LAX-SFO|0.20743189117602162|
|SFO-LAX|0.21310929811278898|
|OAK-LAS| 0.5397299369846683|
+-------+-------------------+


## Top features only
['trailing_avg_fare', 'fr2_fare_prev_shop_day', 'fare_prev_shop_day', 'est_fare_from_next_dept_day', 'est_fare_from_prev_dept_day', 'fare_prev_dept_day', 'avg_fare_dtd', 'fare_next_dept_day']

Overall MAPE 
test: 13.5%
train: 13.1%

not crazy-bad over-fitting

