WITH LIM_DATA AS (
	SELECT *,
		"day" as shopping_date
	FROM "tvlp_ds_air_shopping_pn"."v1_5_parquet"
	WHERE out_origin_city = 'TPA'
		AND in_origin_city = 'ATL'
		AND point_of_sale = 'US'
		AND currency = 'USD'
		AND in_departure_date != 0
-- to expand the query
		AND year = '2022'
		AND month = '10'
-- to limit the data while developing the query
-- 		AND day >= '20221001'
-- 		AND hour = '15'
),
-- explode fares + PTC codes and match them on array index
EXPL_FARES AS (
	SELECT "out_origin_airport", "in_origin_airport", 
	    "ex_ptc", "ex_fare_ptc",
		"out_departure_date", "in_departure_date",
		"out_marketing_cxr", "constricted_search", shopping_date,
		CARDINALITY(out_marketing_cxr) AS num_out_cxr
	FROM LIM_DATA
		CROSS JOIN UNNEST("fare_break_down_by_ptc") WITH ORDINALITY t("ex_fare_ptc", "fare_idx")
		CROSS JOIN UNNEST("response_PTC") WITH ORDINALITY t("ex_ptc", "ptc_idx")
	WHERE ptc_idx = fare_idx
	HAVING ex_ptc = 'ADT'

),
-- explode carrier into columns (out to 2)
EXPL_CXRS AS (
	(
		SELECT e.*,
			out_marketing_cxr [ 1 ] as out_cxr1,
			Null as out_cxr2
		FROM EXPL_FARES e
		WHERE num_out_cxr = 1
	)
	UNION
	(
		SELECT e.*,
			out_marketing_cxr [ 1 ] as out_cxr1,
			out_marketing_cxr [ 2 ] as out_cxr2
		FROM EXPL_FARES e
		WHERE num_out_cxr = 2
	)
)
-- finally, select minimum fare by dates, etc
SELECT ec.out_departure_date, ec.in_departure_date, ec.shopping_date,
    out_origin_airport, in_origin_airport, 
    out_cxr1, ex_ptc, "constricted_search", 
    min_fare
FROM EXPL_CXRS ec,
(
    SELECT "out_departure_date", "in_departure_date", shopping_date, MIN(ex_fare_ptc) as min_fare
    FROM EXPL_CXRS
    GROUP BY "out_departure_date", "in_departure_date", shopping_date
) min_fare
WHERE ec.out_departure_date = min_fare.out_departure_date
    AND ec.in_departure_date = min_fare.in_departure_date
    AND ec.shopping_date = min_fare.shopping_date
    AND ec.ex_fare_ptc = min_fare.min_fare
