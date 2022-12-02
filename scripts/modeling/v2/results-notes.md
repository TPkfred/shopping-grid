

# Lin Regr
overall MAPE: 15.0%
MSE: 136


# Gradient Boosted Trees
GBT-Regr, one model for all markets:
overall MAPE: 14.4%
MSE: 117
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
MSE: 132

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

## PCA-reduced features
fare features: scaled, then PCA k=3
count features: normed, then PCA k=2

MAPE: 17.8%
MSE: 103

+-------+--------------------+
| market|                mape|
+-------+--------------------+
|LHR-EWR| 0.04727946593057232|
|LHR-JFK|0.061310638718680505|
|EWR-LHR| 0.07943416094948834|
|EWR-CDG| 0.11672323247399656|
|LHR-LAX| 0.11787079307728048|
|JFK-LAX| 0.12308869846030551|
|LAX-EWR| 0.12312079557253425|
|LAX-JFK| 0.12366152942106287|
|JFK-LHR|  0.1314034265374511|
|ATL-EWR|  0.2038658012119045|
|LGA-MIA| 0.26249109914440244|
|LAX-SFO| 0.29611076262905295|
|SFO-LAX|  0.3044266296918005|
|OAK-LAS|  0.8917722107613479|
+-------+--------------------+ . 

## tailored dtd clipping

LHR-JFK
+------------------+
|              mape|
+------------------+
|0.0484862382851539|
+------------------+


JFK-LHR
+------------------+
|              mape|
+------------------+
|0.1158944785621252|
+------------------+


JFK-LAX
+------------------+
|              mape|
+------------------+
|0.1003059507563061|
+------------------+


LAX-JFK
+-------------------+
|               mape|
+-------------------+
|0.10566183690555726|
+-------------------+


SFO-LAX
+------------------+
|              mape|
+------------------+
|0.1949559909217171|
+------------------+


# Clip / filter data
Remove "regimes" of data that have generally poor coverage:
- clip out "bad" shop days (10/23 - 10/28)
- filter out dtd > 120 

## Random Forest
Overal MAPE:
Test: 7.56%
Train: 7.61%

+-------+--------------------+
| market|                mape|
+-------+--------------------+
|LHR-LAX| 0.03350816700185495|
|LHR-EWR| 0.03606493183999015|
|LHR-JFK|0.037307598009722005|
|LAX-SFO| 0.05487309683876023|
|SFO-LAX| 0.05703901720419889|
|LAX-JFK|0.057832124712410754|
|JFK-LAX|   0.066143455355757|
|EWR-CDG| 0.06730635087464294|
|JFK-LHR| 0.07775646365052682|
|LGA-MIA| 0.07993429740612892|
|LAX-EWR| 0.08415590873982395|
|ATL-EWR|  0.0985338760326365|
|EWR-LHR| 0.15272428818931122|
|OAK-LAS| 0.16009202246535637|
+-------+--------------------+

Cross Validated & hyper-param's tuned:
Test: 6.10%
Train: 6.32%

Feature importances:
('fare_prev_shop_day', 0.28876858278188594)
('trailing_avg_fare', 0.2705170203129419)
('est_fare_from_prev_dept_day', 0.12518074819893965)
('est_fare_from_next_dept_day', 0.11794167731484437)
('fr2_fare_prev_shop_day', 0.06205557810031882)
('fare_prev_dept_day', 0.05947075382130656)
('fare_next_dept_day', 0.05296685383979888)
('avg_fare_dtd', 0.014328995590173327)
('avg_out_avail_low_prev_shop_day', 0.001117768685376613)
('trailing_std_fare', 0.0009488786145670234)
('is_holiday', 0.0009425474522879261)
('days_til_dept', 0.0008966262354161872)
('trailing_avg_solution_counts', 0.0007885440907385459)
('solution_counts_prev_shop_day', 0.0007490183128382068)
('trailing_avg_shop_counts', 0.0007229720132297499)
('num_itin_prev_shop_day', 0.0007015675151521406)
('shop_counts_prev_shop_day', 0.0006931918617769995)
('dept_dt_dow_int', 0.000470209733807055)
('avg_out_avail_max_prev_shop_day', 0.00041765948757608424)
('fr1_fr2_out_cxrs_same', 0.00017313938089969178)
('fr1_fr2_out_cxrs_overlap', 0.00014766665612436138)

### Feature selection
Top 8 features:
Test: 6.53%
Train: 6.70%

All but bottom 4:
Test: 6.24%
Train: 6.42%

With PCA:
Test: 11.7%

### Final Cross-val
+-------+--------------------+
| market|                mape|
+-------+--------------------+
|LHR-LAX|0.030363407535239677|
|LHR-EWR| 0.03164608422867111|
|LHR-JFK|0.032300538112482964|
|LAX-SFO|0.047152724218127676|
|JFK-LAX| 0.05336179264047349|
|LAX-JFK| 0.05339136702687168|
|EWR-CDG| 0.05404030431479185|
|SFO-LAX|0.054191268556009725|
|LGA-MIA| 0.07054299748646693|
|LAX-EWR|  0.0734402533235749|
|ATL-EWR| 0.08293469643952867|
|EWR-LHR| 0.10704834885074688|
|OAK-LAS| 0.11209563820543827|
|JFK-LHR| 0.25134506093012843|
+-------+--------------------+
Test: 7.45%
Train: 6.02%


Notice how much worse JFK-LHR is! Train-test split must not be determinstic, even with setting a seed.


# Clip / filter data, incl holidays
Remove "regimes" of data that have generally poor coverage, and/or known volatility:
- clip out "bad" shop days (10/23 - 10/28)
- filter out dtd > 120 
- holidays


rf_regression_model = RandomForestRegressor(
    labelCol="fare",
    numTrees=100,
    maxDepth=10,
)


Test MAPE:
+-------------------+
|               mape|
+-------------------+
|0.04606482918705776|
+-------------------+

None
Train MAPE:
+-------------------+
|               mape|
+-------------------+
|0.05180677873932075|
+-------------------+


+-------+--------------------+
| market|                mape|
+-------+--------------------+
|LHR-JFK|0.022949661350584828|
|LHR-LAX| 0.02299483047236049|
|LHR-EWR|0.024029813574555872|
|LAX-SFO|0.024726906221419924|
|SFO-LAX|0.037431899340727354|
|LAX-JFK| 0.03875955611325849|
|JFK-LAX| 0.03973780318294017|
|LAX-EWR| 0.04664281360338371|
|ATL-EWR|0.050093561211971456|
|EWR-CDG|0.051623282610867426|
|EWR-LHR| 0.05373296593450191|
|JFK-LHR| 0.05510647681737911|
|LGA-MIA| 0.06001005919818195|
|OAK-LAS| 0.11414291071147765|
+-------+--------------------+


Feature importance:
('fare_prev_shop_day', 0.323074183167787)
('est_fare_from_next_dept_day', 0.1916686818989582)
('trailing_avg_fare', 0.1756901033254069)
('est_fare_from_prev_dept_day', 0.12918489817963213)
('fr2_fare_prev_shop_day', 0.07505312828438217)
('fare_prev_dept_day', 0.04454737582885741)
('fare_next_dept_day', 0.03410101273695583)
('avg_fare_dtd', 0.020256791223654938)
('days_til_dept', 0.0009100137815682722)
('num_itin_prev_shop_day', 0.0008056025006676735)
('avg_out_avail_max_prev_shop_day', 0.0007206192484696111)
('trailing_avg_solution_counts', 0.0006790735764291154)
('solution_counts_prev_shop_day', 0.0006556602941338979)
('trailing_std_fare', 0.0006434296758017407)
('trailing_avg_shop_counts', 0.0005678902516767344)
('shop_counts_prev_shop_day', 0.0005487338927314758)
('dept_dt_dow_int', 0.00042524814539496644)
('fr1_fr2_out_cxrs_same', 0.000196763598401078)
('fr1_fr2_out_cxrs_overlap', 0.00015695951053312153)
('avg_out_avail_low_prev_shop_day', 0.00011383087855752759)
('is_holiday', 0.0)