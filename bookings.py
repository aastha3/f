# we drop all tables and then create new ones --> inefficient. 
# Jiao's observation  - that numbers change by as much as 3% 

sql_drop_1 = """DROP TABLE product_analytics.ad352_pax_fares"""

#this is to get the API pax_fare for grabexpress bookings 

sql_create_1 = """CREATE TABLE product_analytics.ad352_pax_fares AS
SELECT delivery_booking.booking_code AS booking_code,
       COALESCE(CAST(json_extract (info.quote,'$.amount') AS DOUBLE),pb.fare,0) AS pax_Fare
FROM grab_express.delivery_info info
  INNER JOIN grab_express.delivery_booking ON delivery_booking.delivery_code = info.delivery_code
  LEFT JOIN public.bookings AS pb ON pb.code = delivery_booking.booking_code
WHERE DATE (CONCAT(info.year, '-', info.month, '-', info.day)) >= DATE (date_format(date_parse('2018-01-01', '%Y-%m-%d'), '%Y-%m-%d'))
/*AND   DATE (CONCAT(info.year, '-', info.month, '-', info.day)) < DATE (date_format(date_parse('2019-01-01', '%Y-%m-%d'), '%Y-%m-%d'))*/
AND   info.delivery_code IS NOT NULL
AND   NOT (info.delivery_code = '')
AND   delivery_booking.booking_code IS NOT NULL
AND   NOT (delivery_booking.booking_code = '')
and delivery_booking.year = '2018' and info.year = '2018' and pb.year = '2018' 
and substr(pb.code,1,7) = 'PARTNER'
AND   is_unique_booking = 't'
GROUP BY 1,
         2"""

sql_drop_2 = """DROP TABLE product_analytics.ad352_booking_invoice_tmp"""

sql_create_2 = """CREATE TABLE product_analytics.ad352_booking_invoice_tmp AS
SELECT booking_code,
       a.delivery_code,
       invoice_number
FROM (SELECT delivery_code,
             booking_code
      FROM (SELECT delivery_code,
                   booking_code
            FROM grab_express.delivery_booking  # table1
            WHERE booking_code IS NOT NULL
            AND   delivery_code IS NOT NULL
            AND year = '2018' 
            GROUP BY 1,
                     2
            UNION ALL
            SELECT orderid AS delivery_code,
                   bookingcode AS booking_code  #table2
            FROM grab_datastore.express_event 
            WHERE orderid IS NOT NULL
            AND   bookingcode IS NOT NULL
            AND YEAR  = '2018'
            GROUP BY 1,
                     2)
      GROUP BY 1,
               2) AS a
               
  LEFT JOIN (SELECT delivery_code,
                    invoice_number
             FROM (SELECT delivery_code,
                          CAST(json_extract (info.metadata,'$.invoiceNo') AS VARCHAR) AS invoice_number
                   FROM grab_express.delivery_info info  #table1
                   WHERE CAST(json_extract(info.metadata, '$.invoiceNo') AS VARCHAR) IS NOT NULL and year = '2018'
                   GROUP BY 1,
                            2
                   UNION ALL
                   SELECT order_id AS delivery_code,
                          merchant_order_id AS invoice_number   #table 2
                   FROM grab_express.order_merchant_order_relations
                   UNION ALL
                   SELECT delivery_code,
                          CAST(json_extract (metadata,'$.merchantOrderID') AS VARCHAR) AS invoice_number
                   FROM grab_express.delivery_booking    #table 3
                   where  year = '2018'
                   GROUP BY 1,
                            2)
             GROUP BY 1,
                      2) AS b ON b.delivery_code = a.delivery_code
WHERE invoice_number IS NOT NULL
AND   booking_code IS NOT NULL
AND   booking_code <> ''
AND   invoice_number <> ''
GROUP BY 1,
         2,
         3"""

sql_drop_3 = """DROP TABLE product_analytics.ad352_booking_invoice"""
sql_create_3 = """CREATE TABLE product_analytics.ad352_booking_invoice AS 
SELECT * FROM product_analytics.ad352_booking_invoice_tmp """

sql_drop_4 = """ DROP TABLE product_analytics.ad352_bk_service_type_map_tmp """
sql_create_4 = """CREATE TABLE product_analytics.ad352_bk_service_type_map_tmp  AS
SELECT booking_code,
       MAX(service_type) AS service_type
FROM ( /*removing the db_booking join because the data looks inaccurate*/  
     SELECT booking_code, CAST(json_extract (json_extract (json_extract (METADATA, '$.expressMeta'), '$.jobCard'), '$.jobType') AS INTEGER) 
     AS service_type 
     FROM bs_db.booking AS a
  INNER JOIN product_analytics.taxi_types_v1  AS ge_taxi_types
          ON a.vehicle_type_id = ge_taxi_types.taxi_type_id
         AND DATE (concat (year,'-',month,'-',day)) > DATE ('2018-01-01')
         AND CAST (json_extract (json_extract (json_extract (METADATA,'$.expressMeta'),'$.jobCard'),'$.jobType') AS INTEGER) IS NOT NULL
GROUP BY 1,
         2
UNION ALL
SELECT booking_code,
       service_type
FROM grab_express.delivery_booking
where year = '2018'
GROUP BY 1,
         2
UNION ALL
SELECT bookingcode AS booking_code,
       CASE
         WHEN servicetype = 'INSTANT' THEN 0
         WHEN servicetype = 'SAME_DAY' THEN 1
         ELSE -1
       END AS service_type
FROM grab_datastore.express_event
GROUP BY 1,
         2)
GROUP BY 1
"""
sql_drop_5 = """DROP TABLE product_analytics.ad352_bk_service_type_map"""
sql_create_5 = """ CREATE TABLE product_analytics.ad352_bk_service_type_map  
AS SELECT * FROM product_analytics.ad352_bk_service_type_map_tmp"""

# finsing partner_id=merchant_id for a booking 

sql_drop_14 = """
drop table product_analytics.merchant_mapping
"""

sql_create_14 = """
create table product_analytics.merchant_mapping as 
select code 
, coalesce (cast(db.user_id as varchar), trim( split(bk.code , '-')[2])) as partner_id 
 FROM public.bookings bk
 inner join public.taxi_types_v as ttv on ttv.id = bk.taxi_type_id 
 left join grab_express.delivery_booking as db on db.booking_code = bk.code 
 where substr(code , 1, 7) = 'PARTNER'
 and bk.year = '2018'
 and bk.partition_date > '2018-02-01'
"""


# standard metrics calculation

sql_drop_6 = """DROP TABLE product_analytics.ad352_ge_improvised_tmp"""

sql_create_6 = """
CREATE TABLE product_analytics.ad352_ge_improvised_tmp AS
SELECT CAST(bk.hour_local AS INTEGER) AS hour_local,
       coalesce(merchant_name, 'Unknown') as merchant_name,
       bk.country_name AS country,
       bk.city_name AS city,
       regexp_replace(COALESCE(pickup_area.area,'Other Areas'),'\d') AS pickup_area,
       date_trunc('week',DATE (bk.date_local)) AS week_local,
       COALESCE(CASE WHEN bk.is_unique_booking = 't' AND promo_bookings.promo = TRUE THEN 1 ELSE 0 END,0) AS promo_ride,
       CASE
         WHEN SUBSTRING(bk.code,1,7) = 'PARTNER' THEN 1
         ELSE 0
       END AS api_flag,
       CASE
         WHEN book_service.service_type = 0 THEN 'Instant'
         WHEN book_service.service_type = 1 THEN 'SameDay'
         WHEN book_service.service_type = 2 THEN 'Multimodal'
         WHEN SUBSTRING(bk.code,1,7) = 'PARTNER' THEN 'unmapped'
         ELSE 'APP'
       END AS service_type,
       DATE (bk.date_local) AS date_local,
       SUM(CASE WHEN bk.is_unique_booking = 't' THEN 1 ELSE 0 END) AS unique_bookings,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND promo_bookings.promo = TRUE THEN 1 ELSE 0 END) AS promo_unique_bookings,
       SUM(CASE WHEN ub.first_allocated = 't' THEN 1 ELSE 0 END) AS first_allocated_bookings,
       SUM(CASE WHEN ub.first_allocated = 't' AND bk.is_unique_booking = 't' THEN 1 ELSE 0 END) AS first_allocated_UB,
       SUM(CASE WHEN bk.state NOT IN ('UNALLOCATED') THEN 1 ELSE 0 END) AS allocated_nonUB,
       SUM(CASE WHEN bk.state NOT IN ('UNALLOCATED') AND bk.is_unique_booking = 't' THEN 1 ELSE 0 END) AS allocated_UB,
       SUM(CASE WHEN bk.state = 'UNALLOCATED' THEN 1 ELSE 0 END) AS unallocated_nonUB,
       SUM(CASE WHEN bk.state = 'UNALLOCATED' AND bk.is_unique_booking = 't' THEN 1 ELSE 0 END) AS unallocated_UB,
       SUM(CASE WHEN bk.state IN ('CANCELLED_PASSENGER','CANCELLED_OPERATOR','CANCELLED_DRIVER') THEN 1 ELSE 0 END) AS non_ub_cancelled,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND bk.state IN ('CANCELLED_PASSENGER','CANCELLED_OPERATOR','CANCELLED_DRIVER') THEN 1 ELSE 0 END) AS ub_cancelled,
       SUM(CASE WHEN bk.state = 'CANCELLED_PASSENGER' THEN 1 ELSE 0 END) AS non_ub_pax_cancelled,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND bk.state = 'CANCELLED_PASSENGER' THEN 1 ELSE 0 END) AS ub_pax_cancelled,
       SUM(CASE WHEN bk.state = 'CANCELLED_OPERATOR' THEN 1 ELSE 0 END) AS non_ub_opr_cancelled,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND bk.state = 'CANCELLED_OPERATOR' THEN 1 ELSE 0 END) AS ub_opr_cancelled,
       SUM(CASE WHEN bk.state = 'CANCELLED_DRIVER' THEN 1 ELSE 0 END) AS non_ub_driver_cancelled,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND bk.state = 'CANCELLED_DRIVER' THEN 1 ELSE 0 END) AS ub_driver_cancelled,
       SUM(CASE WHEN ub.first_allocated = 't' AND bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') 
       THEN 1 ELSE 0 END) AS first_allocated_rides,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') THEN 1 ELSE 0 END) AS rides,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') AND promo_bookings.promo = TRUE 
       THEN 1 ELSE 0 END) AS promo_rides,
       SUM(CASE WHEN bk.is_unique_booking = 't' THEN COALESCE(bk.fare,0) ELSE 0 END) AS dax_fare_all_bookings,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') 
       THEN COALESCE(bk.fare,0) ELSE 0 END) AS dax_fare_rides,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') 
       THEN COALESCE(pax_fare_t.pax_fare,bk.fare) ELSE 0 END) AS pax_fare_rides,
       SUM(CASE WHEN bk.is_unique_booking = 't' THEN COALESCE(pax_fare_t.pax_fare,bk.fare) ELSE 0 END) AS pax_fare_all_bookings,
       SUM(CASE WHEN bk.is_unique_booking = 't' THEN COALESCE(bk.fare / exchange_one_usd,0) ELSE 0 END) AS dax_fare_all_bookings_USD,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') 
       THEN COALESCE(bk.fare / exchange_one_usd,0) ELSE 0 END) AS dax_fare_rides_USD,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') 
       THEN COALESCE(COALESCE(pax_fare_t.pax_fare / exchange_one_usd,bk.fare / exchange_one_usd),0) ELSE 0 END) AS pax_fare_rides_USD,
       SUM(CASE WHEN bk.is_unique_booking = 't' THEN COALESCE(COALESCE(pax_fare_t.pax_fare / exchange_one_usd,bk.fare / exchange_one_usd),0) ELSE 0 END) AS pax_fare_all_bookings_USD,
       SUM(CASE WHEN bk.is_unique_booking = 't' THEN COALESCE(bk.distance,0) ELSE 0 END) AS distance_unique_bookings,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND promo_bookings.promo = TRUE THEN COALESCE(bk.distance,0) ELSE 0 END) AS distance_promo_unique_bookings,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') 
       THEN COALESCE(bk.distance,0) ELSE 0 END) AS distance_rides,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') 
       AND promo_bookings.promo = TRUE THEN COALESCE(bk.distance,0) ELSE 0 END) AS distance_promo_rides,
       SUM(1) AS all_bookings,
       SUM(CASE WHEN bk.is_unique_booking = 't' THEN COALESCE(rec.tolls_and_surcharges,0) +COALESCE(bk.fare,0) +COALESCE(rec.booking_fee,0) ELSE 0 END) AS GMV_unique_bookings,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND promo_bookings.promo = TRUE THEN COALESCE(rec.tolls_and_surcharges,0) +COALESCE(bk.fare,0) +COALESCE(rec.booking_fee,0) ELSE 0 END) AS GMV_promo_unique_bookings,
       SUM(CASE WHEN bk.state IN ('CANCELLED_DRIVER') THEN 1 ELSE 0 END) AS job_cancelled_by_dax,
       SUM(CASE WHEN bk.state IN ('CANCELLED_PASSENGER') THEN 1 ELSE 0 END) AS job_cancelled_by_pax,
       SUM(CASE WHEN bk.state IN ('CANCELLED_OPERATOR') THEN 1 ELSE 0 END) AS job_cancelled_by_ops,
       SUM(CASE WHEN bk.state IN ('CANCELLED_DRIVER') AND bk.is_unique_booking = 't' THEN 1 ELSE 0 END) AS booking_cancelled_by_dax,
       SUM(CASE WHEN bk.state IN ('CANCELLED_PASSENGER') AND bk.is_unique_booking = 't' THEN 1 ELSE 0 END) AS booking_cancelled_by_pax,
       SUM(CASE WHEN bk.state IN ('CANCELLED_OPERATOR') AND bk.is_unique_booking = 't' THEN 1 ELSE 0 END) AS booking_cancelled_by_ops,
       SUM(CASE WHEN cmet.awarded_time IS NOT NULL THEN date_diff ('second',date_parse (ub.first_at,'%Y-%m-%d %H:%i:%s'),cmet.awarded_time) ELSE 0 END) AS time_to_allocation,
       SUM(CASE WHEN cmet.awarded_time IS NOT NULL AND cmet.cancel_passenger_time IS NOT NULL THEN date_diff ('second',cmet.awarded_time,cmet.cancel_passenger_time) WHEN cmet.awarded_time IS NOT NULL AND cmet.cancel_driver_time IS NOT NULL THEN date_diff ('second',cmet.awarded_time,cmet.cancel_driver_time) WHEN cmet.awarded_time IS NOT NULL AND cmet.cancel_operator_time IS NOT NULL THEN date_diff ('second',cmet.awarded_time,cmet.cancel_operator_time) ELSE 0 END) AS time_to_cancellation,
                   SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') THEN COALESCE(bk.commission,0) ELSE 0 END) AS commission_rides,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') THEN COALESCE(promo_bookings.promo_expense_local,0) ELSE 0 END) AS promo_expense_rides,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') THEN COALESCE(incentive_bookings.incentive_payout,0) ELSE 0 END) AS incentive_payout_rides,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') THEN COALESCE(bk.fare,0) ELSE 0 END) AS fare_rides,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') AND promo_bookings.promo = TRUE THEN COALESCE(bk.fare,0) ELSE 0 END) AS fare_promo_rides,
       SUM(CASE WHEN bk.is_unique_booking = 't' THEN COALESCE(bk.fare,0) ELSE 0 END) AS fare_unique_bookings,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND promo_bookings.promo = TRUE THEN COALESCE(bk.fare,0) ELSE 0 END) AS fare_promo_unique_bookings,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') THEN COALESCE(rec.tolls_and_surcharges,0) +COALESCE(bk.fare,0) +COALESCE(rec.booking_fee,0) ELSE 0 END) AS GMV_rides,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') AND promo_bookings.promo = TRUE THEN COALESCE(rec.tolls_and_surcharges,0) +COALESCE(bk.fare,0) +COALESCE(rec.booking_fee,0) ELSE 0 END) AS GMV_promo_rides,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') THEN COALESCE(bk.commission / exchange_one_usd,0) ELSE 0 END) AS commission_rides_USD,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') THEN COALESCE(promo_bookings.promo_expense_local / exchange_one_usd,0) ELSE 0 END) AS promo_expense_rides_USD,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') THEN COALESCE(incentive_bookings.incentive_payout / exchange_one_usd,0) ELSE 0 END) AS incentive_payout_rides_USD,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') THEN COALESCE(bk.fare / exchange_one_usd,0) ELSE 0 END) AS fare_rides_USD,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') AND promo_bookings.promo = TRUE THEN COALESCE(bk.fare / exchange_one_usd,0) ELSE 0 END) AS fare_promo_rides_USD,
       SUM(CASE WHEN bk.is_unique_booking = 't' THEN COALESCE(bk.fare / exchange_one_usd,0) ELSE 0 END) AS fare_unique_bookings_USD,
       SUM(CASE WHEN bk.is_unique_booking = 't' AND promo_bookings.promo = TRUE THEN COALESCE(bk.fare / exchange_one_usd,0) ELSE 0 END) AS fare_promo_unique_bookings_USD,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') THEN COALESCE(rec.tolls_and_surcharges / exchange_one_usd,0) +COALESCE(bk.fare / exchange_one_usd,0) +COALESCE(rec.booking_fee / exchange_one_usd,0) ELSE 0 END) AS GMV_rides_USD,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') AND promo_bookings.promo = TRUE THEN COALESCE(rec.tolls_and_surcharges / exchange_one_usd,0) +COALESCE(bk.fare / exchange_one_usd,0) +COALESCE(rec.booking_fee / exchange_one_usd,0) ELSE 0 END) AS GMV_promo_rides_USD,
       /* completed , cancelled, fare , distance*/ SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') AND bk.is_unique_booking = 't' THEN bk.distance ELSE NULL END) AS completed_distance,
       SUM(CASE WHEN bk.state IN ('ADVANCE_AWARDED','AWARDED','COMPLETED','DROPPING_OFF','PICKING_UP') AND bk.is_unique_booking = 't' THEN bk.fare ELSE NULL END) AS completed_fare,
       SUM(CASE WHEN bk.state IN ('CANCELLED_DRIVER','CANCELLED_PASSENGER','CANCELLED_OPERATOR') AND bk.is_unique_booking = 't' THEN bk.distance ELSE NULL END) AS cancelled_distance,
       SUM(CASE WHEN bk.state IN ('CANCELLED_DRIVER','CANCELLED_PASSENGER','CANCELLED_OPERATOR') AND bk.is_unique_booking = 't' THEN bk.fare ELSE NULL END) AS cancelled_fare
FROM public.bookings bk
  INNER JOIN product_analytics.taxi_types_v1  AS ge_taxi_types ON bk.taxi_type_id = ge_taxi_types.taxi_type_id

/* 
 left join product_analytics.merchants1 on cast(merchants1.merchant_grab_id as varchar) = cast(substr(bk.code, 9, 8) as varchar)
*/

-- this is created right above this 
LEFT JOIN product_analytics.merchant_mapping ON merchant_mapping.code = bk.code 
LEFT JOIN grab_express.merchants ON  CAST(merchants.merchant_grab_id AS VARCHAR) = merchant_mapping.partner_id

-- convert fare to USD
  LEFT JOIN datamart.ref_exchange_rates ex
         ON ex.country_id = bk.country_id
        AND date_trunc ('month',DATE (bk.date_local)) = date_trunc ('month',ex.end_date)
        AND date_trunc ('year',DATE (bk.date_local)) = date_trunc ('year',ex.end_date)

-- service type mapping 
  LEFT JOIN product_analytics.ad352_bk_service_type_map AS book_service ON book_service.booking_code = bk.code

-- booking to invoice_id mapping 
  LEFT JOIN product_analytics.ad352_booking_invoice AS book_inv ON bk.code = book_inv.booking_code

  LEFT JOIN product_analytics.ad352_pax_fares AS pax_fare_t ON pax_fare_t.booking_code = bk.code

  LEFT JOIN (SELECT receipts.booking_code,
                    receipts.tolls_and_surcharges,
                    receipts.booking_fee
             FROM public.receipts
             WHERE CONCAT(receipts.year, '-', receipts.month, '-', receipts.day) >= 
             date_format(date_parse('2018-02-01', '%Y-%m-%d') -INTERVAL '1' DAY, '%Y-%m-%d')
             ) AS rec ON rec.booking_code = bk.code

  LEFT JOIN (SELECT last_id,
                    first_at,
                    first_allocated
             FROM public.unique_bookings
             WHERE /*incremental*/  CONCAT(YEAR, '-', MONTH, '-', DAY) >= 
             date_format(date_parse('2018-02-01', '%Y-%m-%d') -INTERVAL '1' DAY, '%Y-%m-%d')

             ) ub ON (bk.id = ub.last_id)
             
LEFT JOIN (SELECT code,
                    promo_expense_local,
                    TRUE AS promo
             FROM datamart.raw_promo_bookings
             WHERE date_local >= date_parse('2018-02-01', '%Y-%m-%d')
             ) AS promo_bookings ON promo_bookings.code = bk.code

  LEFT JOIN (SELECT booking_code,
                    SUM(payout_per_ride) AS incentive_payout
             FROM transforms.incentives_payout_per_ride
             WHERE date_partition >= date_format(date_parse('2018-02-01', '%Y-%m-%d') -INTERVAL '1' DAY, '%Y-%m-%d')

             GROUP BY 1) AS incentive_bookings ON incentive_bookings.booking_code = bk.code

  LEFT JOIN (SELECT area_map.*,
                    area_lat.latitude,
                    area_lat.longitude
             FROM geohash.area_map
               LEFT JOIN (SELECT country_id,
                                 city_id,
                                 area,
                                 AVG(latitude) AS latitude,
                                 AVG(longitude) AS longitude
                          FROM geohash.polygon_boundaries
                          GROUP BY 1,
                                   2,
                                   3) AS area_lat
                      ON area_map.country_id = area_lat.country_id
                     AND area_map.city_id = area_lat.city_id
                     AND area_map.area = area_lat.area) AS pickup_area
         ON (pickup_area.geohash = bk.pickup_geohash
        AND pickup_area.city_id = bk.city_id
        AND pickup_area.country_id = bk.country_id)


  LEFT JOIN product_analytics.taxi_types_v1  as tt ON (bk.taxi_type_id = tt.taxi_type_id)

  LEFT JOIN public.test_bookings tb ON (bk.code = tb.code)

  LEFT JOIN (SELECT booking_code,
                    awarded_time,
                    cancel_passenger_time,
                    cancel_driver_time,
                    cancel_operator_time,
                    partition_date
             FROM public.candidate_metadata as cmet
inner join public.taxi_types_v as ttv on cast(ttv.id as varchar) = cmet.vehicle_type_id 
             WHERE winner = 'true' and taxi_type_simple = 'GrabExpress'
             AND   partition_date >= date_format(date_parse('2018-02-01', '%Y-%m-%d') -INTERVAL '1' DAY, '%Y-%m-%d')

             ) cmet
         ON bk.code = cmet.booking_code
        AND bk.partition_date = cmet.partition_date
WHERE tb.code IS NULL
AND   ((bk.created_at_local >= tt.start_at AND bk.created_at_local < tt.end_at) OR tt.start_at IS NULL)
AND   bk.country_id NOT IN (7, 8)
AND   bk.partition_date >= date_format(date_parse('2018-02-01', '%Y-%m-%d') -INTERVAL '1' DAY, '%Y-%m-%d')
AND   bk.date_local >= '2018-02-01'
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9,
         10 """
sql_drop_7 = """ DROP TABLE product_analytics.ad352_ge_improvised"""

sql_create_7 = """CREATE TABLE product_analytics.ad352_ge_improvised  AS SELECT * FROM product_analytics.ad352_ge_improvised_tmp"""

sql_drop_8 = """ DROP TABLE product_analytics.ad352_tmp1_tmp """

sql_create_8 = """
CREATE TABLE product_analytics.ad352_tmp1_tmp 
AS
SELECT book_inv.invoice_number,
       COUNT(DISTINCT (pb.code)) AS non_UB,
       COUNT(DISTINCT (CASE WHEN pb.is_unique_booking = 't' THEN pb.code ELSE NULL END)) AS unique_bookings
FROM public.bookings pb
  INNER JOIN product_analytics.taxi_types_v1  AS aastha_ge_taxi_types ON pb.taxi_type_id = aastha_ge_taxi_types.taxi_type_id
  LEFT JOIN product_analytics.ad352_bk_service_type_map AS book_service ON book_service.booking_code = pb.code
  LEFT JOIN product_analytics.ad352_booking_invoice AS book_inv ON pb.code = book_inv.booking_code
  LEFT JOIN grab_express.delivery_booking AS booking ON booking.booking_code = pb.code
  LEFT JOIN grab_express.delivery_info AS info ON booking.delivery_code = info.delivery_code
  LEFT JOIN public.unique_bookings_new AS fta ON fta.booking_id = pb.id
  LEFT JOIN test_bookings AS c
         ON c.code = pb.code
        AND c.code IS NULL
  LEFT JOIN product_analytics.merchants1 merchant ON CAST (merchant.merchant_grab_id AS VARCHAR) = substr (pb.code,9,8)
WHERE c.code IS NULL
AND   pb.PARTITION_DATE >= '2018-01-01' and booking.year = '2018' and info.year = '2018'
AND   SUBSTRING(pb.code, 1, 7) = 'PARTNER'
GROUP BY 1 """

sql_drop_9 = """DROP TABLE product_analytics.ad352_tmp1"""

sql_create_9 = """CREATE TABLE product_analytics.ad352_tmp1  AS SELECT * FROM product_analytics.ad352_tmp1_tmp"""

sql_drop_10 = """DROP TABLE product_analytics.ad352_ge_invoice_pivot2_tmp"""
sql_create_10 = """

CREATE TABLE product_analytics.ad352_ge_invoice_pivot2_tmp 

AS
SELECT non_UB,
       unique_bookings,
       final_invoice_state,
       time_to_allocate,
       time_to_pick,
       time_to_drop,
       time_to_allocate1,
       time_to_pick1,
       time_to_drop1,
       time_to_fail,
       tt_allocate,
       country_name,
       city_name,
       date_local,
       merchant_name,
       service_type,
       distance,
       wrap.invoice_hour_local AS invoice_hour_local,
       COUNT(DISTINCT (invoice_number)) AS invoices,
       SUM(dax_fare) AS dax_fare,
       SUM(dax_fare_usd) AS dax_fare_usd,
       SUM(pax_fare) AS pax_fare,
       SUM(pax_fare_usd) AS pax_fare_usd
FROM
-- start of wrap
(SELECT final_state.invoice_number,
        --first_time_stamp,
        final_state.final_invoice_state,
        --       --pick_up_time_local,--       --fail_time,
        allocation_time,
        first_state.invoice_hour_local,
        --       --created_at,      
        date_diff('second',first_time_stamp,allocation_time) / 60 AS time_to_allocate,
        --pickup_time,           
        date_diff('second',allocation_time,pickup_time) / 60 AS time_to_pick,
        --dropoff_time,       
        date_diff('second',allocation_time,dropoff_time) / 60 AS time_to_drop,
        date_diff('second',first_time_stamp,allocation_time) / 60 AS time_to_allocate1,
        --pickup_time,           
        date_diff('second',first_time_stamp,pickup_time) / 60 AS time_to_pick1,
        --dropoff_time,       
        date_diff('second',first_time_stamp,dropoff_time) / 60 AS time_to_drop1,
        date_diff('second',first_time_stamp,fail_time) / 60 AS time_to_fail,
        date_diff('second',first_time_stamp,created_at) / 60 AS tt_allocate,
        non_ub,
        unique_bookings,
        dax_fare,
        dax_fare_usd,
        pax_Fare,
        final_state.pax_fare_usd,
        country_name,
        city_name,
        DATE (first_time_stamp) AS date_local,
        merchant_name,
        service_type,
        distance
 FROM 
 (SELECT invoice_number,
         final_state.booking_code AS final_booking_code,
         state AS final_invoice_state,
         distance,
         try(date_parse (REPLACE(created_at,'.000',''),'%Y-%m-%d %H:%i:%s')) AS created_at,
         dax_fare,
         dax_fare_usd,
         pax_Fare,
         pax_fare_usd,
         commission,
         try(date_parse (REPLACE(pick_up_time_local,'.000',''),'%Y-%m-%d %H:%i:%s')) AS pick_up_time_local,
         allocation_time,
         pickup_time,
         dropoff_time,
         fail_time,
         country_name,
         city_name,
         date_local,
         merchant_name,
         service_type
  FROM (SELECT invoice_number,
               bookings.code AS booking_code,
               bookings.state,
               bookings.created_at,
               distance,
               bookings.fare AS dax_fare,
               COALESCE(bookings.fare / exchange_one_usd,0) AS dax_fare_usd,
               COALESCE(CAST(json_extract (info.quote,'$.amount') AS DOUBLE),bookings.fare) AS pax_fare,
               COALESCE(CAST(json_extract (info.quote,'$.amount') AS DOUBLE) / exchange_one_usd,bookings.fare / exchange_one_usd) AS pax_fare_usd,
               commission,
               pick_up_time_local,
               country_name,
               city_name,
               date_local,
               merchant_name,
               book_service.service_type,
               TRY(date_parse (REPLACE(REPLACE(REPLACE(CAST(json_extract (booking.timeline,'$.allocate') AS VARCHAR),'T',' '),'Z',''),'.000',''),'%Y-%m-%d %H:%i:%s')) AS allocation_time,
               TRY(date_parse (REPLACE(REPLACE(REPLACE(CAST(json_extract (booking.timeline,'$.pickup') AS VARCHAR),'T',' '),'Z',''),'.000',''),'%Y-%m-%d %H:%i:%s')) AS pickup_time,
               TRY(date_parse (REPLACE(REPLACE(REPLACE(CAST(json_extract (booking.timeline,'$.dropoff') AS VARCHAR),'T',' '),'Z',''),'.000',''),'%Y-%m-%d %H:%i:%s')) AS dropoff_time,
               TRY(date_parse (REPLACE(REPLACE(REPLACE(CAST(json_extract (booking.timeline,'$.fail') AS VARCHAR),'T',' '),'Z',''),'.000',''),'%Y-%m-%d %H:%i:%s')) AS fail_time,
               ROW_NUMBER() OVER (PARTITION BY invoice_number ORDER BY bookings.created_at DESC) AS r_num

        FROM public.bookings AS bookings
          INNER JOIN product_analytics.taxi_types_v1  AS aastha_ge_taxi_types ON bookings.taxi_type_id = aastha_ge_taxi_types.taxi_type_id
         /* LEFT JOIN product_analytics.merchants1 merchant ON CAST (merchant.merchant_grab_id AS VARCHAR) = substr (bookings.code,9,8)
          */
          LEFT JOIN product_analytics.merchant_mapping ON merchant_mapping.code = bookings.code 
          LEFT JOIN grab_express.merchants ON  CAST(merchants.merchant_grab_id AS VARCHAR) = merchant_mapping.partner_id       
          LEFT JOIN product_analytics.ad352_bk_service_type_map AS book_service ON book_service.booking_code = bookings.code
          LEFT JOIN product_analytics.ad352_booking_invoice AS book_inv ON bookings.code = book_inv.booking_code
          LEFT JOIN public.test_bookings AS c ON c.code = bookings.code
          LEFT JOIN grab_express.delivery_booking AS booking ON booking.booking_code = bookings.code
          LEFT JOIN grab_express.DELIVERY_INFO AS info ON info.delivery_code = booking.delivery_code
          LEFT JOIN datamart.ref_exchange_rates ex
                 ON ex.country_id = bookings.country_id
                AND date_trunc ('month',DATE (bookings.date_local)) = date_trunc ('month',ex.end_date)
                AND date_trunc ('year',DATE (bookings.date_local)) = date_trunc ('year',ex.end_date)
        WHERE c.code IS NULL
        AND   SUBSTRING(bookings.code, 1, 7) = 'PARTNER' and booking.year = '2018' and INFO.year = '2018'
        AND   bookings.PARTITION_DATE >= '2018-01-01'
        GROUP BY 1,
                 2,
                 3,
                 4,
                 5,
                 6,
                 7,
                 8,
                 9,
                 10,
                 11,
                 12,
                 13,
                 14,
                 15,
                 16,
17,18,19 ,                  20) AS final_state
  WHERE r_num = 1) AS final_state
   LEFT JOIN product_analytics.ad352_tmp1 AS t1 ON t1.invoice_number = final_state.invoice_number
   LEFT JOIN (SELECT invoice_number,
                     booking_code AS first_booking_code,
                     date_parse(REPLACE(created_at,'.000',''),'%Y-%m-%d %H:%i:%s') AS first_time_stamp,
                     invoice_hour_local
              FROM (SELECT invoice_number,
                           bookings.code AS booking_code,
                           bookings.created_at,
                           bookings.hour_local AS invoice_hour_local,
                           ROW_NUMBER() OVER (PARTITION BY invoice_number ORDER BY bookings.created_at ASC) AS r_num
                    FROM public.bookings AS bookings

                      INNER JOIN 
                       product_analytics.taxi_types_v1  AS aastha_ge_taxi_types ON bookings.taxi_type_id = aastha_ge_taxi_types.taxi_type_id

                      LEFT JOIN product_analytics.ad352_bk_service_type_map AS book_service ON book_service.booking_code = bookings.code

                      LEFT JOIN product_analytics.ad352_booking_invoice AS book_inv ON bookings.code = book_inv.booking_code

                      LEFT JOIN public.test_bookings AS c ON c.code = bookings.code

                    WHERE c.code IS NULL
                    AND   SUBSTRING(bookings.code, 1, 7) = 'PARTNER'
                    AND   bookings.partition_date >= '2018-01-01'
                    GROUP BY 1,
                             2,
                             3,
                             4)
              WHERE r_num = 1) AS first_state ON first_state.invoice_number = final_state.invoice_number
 GROUP BY 1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          15,
          16,
          17,
          18,
          19,
          20,
          21,
          22,
          23,
          24) AS wrap
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9,
         10,
         11,
         12,
         13,
         14,
         15,
         16, 17,18 """
         
         
sql_drop_11 = """DROP TABLE product_analytics.ad352_ge_invoice_pivot2"""
sql_create_11 = """
CREATE TABLE product_analytics.ad352_ge_invoice_pivot2 
AS
SELECT *
FROM product_analytics.ad352_ge_invoice_pivot2_tmp"""

sql_drop_12 = """ 
drop table product_analytics.taxi_types_v1 
"""
sql_create_12 = """
create table product_analytics.taxi_types_v1  as
select
id as taxi_type_id,
country_id ,
city_id,
name,
start_at,
end_at,
taxi_type_simple
from public.taxi_types_v
 WHERE taxi_type_simple = 'GrabExpress' """


sql_drop_13 = """
drop table product_analytics.merchants1
"""
sql_create_13 = """
create table product_analytics.merchants1 as 
select * from grab_express.merchants
"""






allsql =[sql_drop_1, sql_create_1, #2
sql_drop_12, sql_create_12, #4
sql_drop_13, sql_create_13, #6
sql_drop_2, sql_create_2, #8
sql_drop_3, sql_create_3, #10
sql_drop_4, sql_create_4, #12 --
sql_drop_5, sql_create_5, #14
sql_drop_6, sql_create_6, #16 --
sql_drop_7, sql_create_7, #18 --
sql_drop_8, sql_create_8, #20 --
sql_drop_9, sql_create_9, #22 --
sql_drop_10, sql_create_10, #24 --
sql_drop_11, sql_create_11] #26

messages = ["sql drop 1", "sql create 1", "sql drop 2" ,"sql create 2" , "sql drop 3", "sql create 3" , "sql drop 12" , "sql create 12", "sql drop 13" , "sql create 13" , "sql drop 4" , "sql create 4", "sql drop 5", "sq create 5", "sq drop 6" ,"sql create 6" , "sql drop 7", "sql create 7" ,"sql drop 8" , "sql create 8","sql drop 9", "sql create 9",  "sql drop 10" ,"sql create 10" ,"sql drop 11", "sql create 11" ]

