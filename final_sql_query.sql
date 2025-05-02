select
  distinct farmerId,
  productName,
  skuCode,
  quantity,
  
  case
    when state = 'MH' then 'B2BMH'
    when state = 'KR' then 'B2BKA'  
    when state = 'MP' then 'B2BMP'
    when state = 'TG' then 'B2BTS'
    when state = 'RJ' then 'B2BRJ'
    when state = 'BH' then 'B2BBH'
    when state = 'CG' then 'B2BCT'
    when state = 'GJ' then 'B2BGJ'
    when state = 'HR' then 'B2BHR'
    when state = 'UP' then 'B2BUP'
    when state = 'AD' then 'B2BAD'
    else ''
  end as source,
  appliedOffer,
  state,

  businessCategory

from
(
Select distinct *

from
(

Select * from
(

SELECT 
      San.reference_customer_id,
      san.name,
      san.partner_name,
      san.status,
      san.created_on,
      san.address_district,
      san.address_state,
      san.totalCreditLimit,
      san.businessCategory,
      dip.order_month,
      dip.Revenue

From


(SELECT reference_customer_id,name,partner_name,status,contacts_mobile_number,DATE(created_on) AS created_on,address_district,address_state,totalCreditLimit,
  businessCategory
FROM replica_galaxy_views.institution
--FROM galaxy_views.institution

WHERE status = 'ACTIVE'
AND archive = FALSE
AND LOWER(ancestor_institutions_name) LIKE '%sathi%'
AND date(created_on) >= '2024-01-01'
And name NOT LIKE '%Testing%'


 ) As San

LEFT JOIN (

  SELECT DISTINCT owner_id,State_,order_month,Sum(Sku_Revenue) As Revenue

From

(SELECT
T2.owner_id,
Right(T2.initiating_source,2) As State_,
date(T2.confirmed_on) As order_date,
date_trunc(date(T2.confirmed_on),month) order_month,
-- T1.quantity As Sku_Qyt,
T1.Total_Price As Sku_Revenue,      
T2.Unicommerce_status,
       
from `prod_db_views.order_management_orderitem` as T1

left join (Select distinct sales_order_id,unicommerce_id, initiating_source,
owner_id, channel,unicommerce_status,confirmed_on,order_type from `prod_db_views.order_management_order`) as T2

on T1.order_id = T2.sales_order_id
   
where T2.confirmed_on >= '2024-01-01'
and  T2.unicommerce_status not in ('APP_UNVERIFIED', 'CANCELLED','FUTURE ORDER', 'MOB_APP_UNVERIFIED','RECEIVED','RETURNED','PICKUPSCHEDULED','ERROR','PENDING','WAITING_FOR_APPROVAL','ONLINE_PAYMENT_PENDING')
and T2.initiating_source Like "%B2B%"
-- and T1.item_sku = 'AGS-HW-1480'
and T2.order_type not in ('RETURNED_ORDER'))

group by 1,2,3

) as dip

On san.reference_customer_id = dip.owner_id)

where Revenue IS NULL
and safe_cast(REGEXP_REPLACE(totalCreditLimit, r'\..*', '') as int64) >= 10
 
and  reference_customer_id in
(SELECT  farmer_id

from
(
SELECT farmer_id,
max(rank) as max_pg

from


(select *except(facility,total_revenue,source),

from

(select a.*, rank()over(partition by a.farmer_id order by a.total_revenue desc) as rank from (select a.*,b.facility from (select a.*except(source),b.source_code as source from(select a.* from(select a.*,b.*except(state,cartCriteria_constraints_constraintList_params_itemQuanList_skuCode) from(select *except(item_Code)

from
( select a.* from

(select a.* from(select a.*,b.product_group,b.item_sku,b.total_revenue from(select distinct farmer_id,state,territory,revised_district from `offline_team.okr_data_live` where status='ACTIVE')
a

inner join
(
----districts top products ----
select distinct *except(rank) from (select a.*,rank()over(partition by a.state,territory,revised_district,product_group order by total_Revenue desc) as rank from(select state,territory,revised_district,product_group,item_Sku,round(sum(total_revenue))total_revenue from
(


------district top products apr may-----
select distinct state, territory,revised_district,product_group,item_sku,sum(total_Revenue)total_Revenue from(
select a.*,b.* from
(select a.*,b.*except(order_id) from(select owner_id,sales_order_id, from `prod_db_views.order_management_order` where  unicommerce_status not in ('APP_UNVERIFIED', 'CANCELLED','FUTURE ORDER', 'MOB_APP_UNVERIFIED','RECEIVED','RETURNED','PICKUPSCHEDULED','ERROR','PENDING') and order_type !="RETURN_ORDER" 
AND DATE(created_on) BETWEEN 
  DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH), INTERVAL 0 MONTH)
AND 
  LAST_DAY(DATE_ADD(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH), INTERVAL 1 MONTH))

--DATE(created_on)between '2024-04-01' and '2024-05-31' 

AND LEFT(SOURCE,3)="B2B"
              ) a
              inner join
 (select order_id,item_Sku,product_group,sum(total_price)total_revenue from `prod_db_views.order_management_orderitem` oi
left join `revenue_and_growth_team.sku_cat_repo` as cr on cr.item_sku_code = oi.Item_SKU
 where DATE(created_on) BETWEEN 
  DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH), INTERVAL 0 MONTH)
AND 
  LAST_DAY(DATE_ADD(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH), INTERVAL 1 MONTH))

 --date(created_on) between '2024-04-01' and '2024-05-31'
  and product_group!="PSP" and  upper(cr.item_sku_code) not like "%MKT%" and lower(cr.category) not in ("hw","kit","seeds","ah","elec") and lower(Product_group) not like("%others%")and pl_NPL="PL"
  group by 1,2,3)b
  on a.sales_order_id=b.order_id) a  
  inner join (select farmer_id,state,territory,revised_district from `offline_team.okr_data_live`)b
  on a.owner_id=b.farmer_id)
  group by 1,2,3,4,5

  union all

  ------district top products last 45 days -----
select distinct state, territory,revised_district,product_group,item_sku,sum(total_Revenue)total_Revenue from(
select a.*,b.* from
(select a.*,b.*except(order_id) from
(select owner_id,sales_order_id, from `prod_db_views.order_management_order` where  unicommerce_status not in ('APP_UNVERIFIED', 'CANCELLED','FUTURE ORDER', 'MOB_APP_UNVERIFIED','RECEIVED','RETURNED','PICKUPSCHEDULED','ERROR','PENDING') and order_type !="RETURN_ORDER" 
AND  date(created_on) between date_sub(current_date(), interval 45 day) and current_date()
--DATE(created_on)between '2025-03-10' and '2025-04-24' 
AND LEFT(SOURCE,3)="B2B"
) a
              inner join
 (select order_id,item_Sku,product_group,sum(total_price)total_revenue from `prod_db_views.order_management_orderitem` oi
left join `revenue_and_growth_team.sku_cat_repo` as cr on cr.item_sku_code = oi.Item_SKU
 where 
  date(created_on) between date_sub(current_date(), interval 45 day) and current_date()
 --date(created_on) between '2025-03-10' and '2025-04-24'
  and product_group!="PSP" and  upper(cr.item_sku_code) not like "%MKT%" and lower(cr.category) not in ("hw","kit","seeds","ah","elec") and lower(Product_group) not like("%others%") and pl_NPL="PL"
  group by 1,2,3)b
  on a.sales_order_id=b.order_id) a  
  inner join (select farmer_id,state,territory,revised_district from `offline_team.okr_data_live`)b
  on a.owner_id=b.farmer_id)
  group by 1,2,3,4,5
)group by 1,2,3,4,5) a )where rank =1

)b
on  a.territory=b.territory and a.revised_district=b.revised_district) a

------partner purchased products last 60 days-----

left join
(
select distinct farmer_id ,product_group,
from
(
select a.*,b.* from
(select a.*,b.*except(order_id)
from
(select owner_id,sales_order_id, from `prod_db_views.order_management_order` where  unicommerce_status not in ('APP_UNVERIFIED', 'CANCELLED','FUTURE ORDER', 'MOB_APP_UNVERIFIED','RECEIVED','RETURNED','PICKUPSCHEDULED','ERROR','PENDING') and order_type !="RETURN_ORDER" 
AND 
 date(created_on) between date_sub(current_date(), interval 60 day) and current_date()
--DATE(created_on)between '2025-02-23' and '2025-04-24' 

AND LEFT(SOURCE,3)="B2B"

) a
inner join
 (select order_id,item_Sku,product_group,sum(total_price)total_revenue from `prod_db_views.order_management_orderitem` oi
left join `revenue_and_growth_team.sku_cat_repo` as cr on cr.item_sku_code = oi.Item_SKU
 where 
 date(created_on) between date_sub(current_date(), interval 60 day) and current_date()
 --date(created_on) between '2025-02-23' and '2025-04-24'
  and product_group!="PSP" and  upper(cr.item_sku_code) not like "%MKT%" and lower(cr.category) not in ("hw","kit","seeds","ah","elec")
  and pl_NPL="PL"
  group by 1,2,3
  )b
  on a.sales_order_id=b.order_id) a  
  inner join (select farmer_id,state,territory,revised_district from `offline_team.okr_data_live`)b
  on a.owner_id=b.farmer_id
  )
)b
on a.farmer_id =b.farmer_id and a.product_group=b.product_Group
where b.farmer_id is null

)  a
-- WHERE NOT (
--     (product_group IN ("Amaze - X", "Rapigen", "TeBull", "Stelar", "Shutter") AND state = "MP") OR
--     (product_group IN ("Florofix", "TMT 70", "Kleenweed", "Waaris") AND state = "MH") OR
--     (product_group IN ("Metalgro", "Pure Kelp", "Shutter", "Sulphur Maxx") AND state = "UP") OR
--     (product_group IN ("Roztam", "Rapigen", "Metalgro", "Kill X", "Amaze - X") AND state = "BH") OR
--     (product_group IN ("Florence", "Roztam", "Kill X", "Rapigen", "Nutripro") AND state = "GJ") OR
--     (product_group IN ("Faster", "Kill X", "Roztam", "Dragnet", "TMT 70", "Rule") AND state = "RJ")
-- )

)a
left join (SELECT  distinct item_code,name,moq  as qty from pristine_wms_views.item_mst)b
    on a.item_sku=b.item_Code
)a
left join (SELECT
  uuid,
  cartCriteria_constraints_constraintList_params_itemQuanList_skuCode,right(source,2) as state,
  source
FROM (
  SELECT
    uuid,
    cartCriteria_constraints_constraintList_params_itemQuanList_skuCode,
    SPLIT(REPLACE(REPLACE(sources, '[', ''), ']', ''), ',') AS source_array
  FROM
    agrostar-data.prod_db_views.promotion
  WHERE
    LOWER(sources) LIKE "%b2b%"
    AND status IN ("ACTIVATED")
    -- and date(createdon)='2024-12-12'
    AND DATE(validTo) > CURRENT_DATE() - 1
    AND type = "OFFER"
)
CROSS JOIN UNNEST(source_array) AS source)b
on a.state=b.state and a.item_sku=b.cartCriteria_constraints_constraintList_params_itemQuanList_skuCode

)
a
left join
(SELECT DISTINCT
  CASE
    WHEN RIGHT(source, 2) = 'TS' THEN 'TG'
    WHEN RIGHT(source, 2) = 'CT' THEN 'CG'
    WHEN RIGHT(source, 2) = 'KA' THEN 'KR'
    ELSE RIGHT(source, 2)
  END AS state,
  skuCode,

FROM agrostar-data.prod_db_views.blockedSKUs
where date(uploadTime)='2024-01-08'
)b
     on a.state=b.state and a.item_sku=b.skucode
     where b.state is null) a
     left join
     (
      SELECT DISTINCT
  CASE
    WHEN state = 'TS' THEN 'TG'
    WHEN state = 'CT' THEN 'CG'
    WHEN state = 'KA' THEN 'KR'
    ELSE state
  END AS state,*except(state)
      from
      (select aa.product_id, cc.sku_code, bb.source_code, case when source_code not in ("B2B", "B2B-IGNORE") Then Right(source_code,2) ELSE null end  as state,
case when aa.is_enabled = 1 then 'active' else 'inactive' end as cms_active,


from catalog_views.catalog_management_productsourceassociation aa
left join catalog_views.catalog_management_source bb on aa.applicable_source_id = bb.id
left join catalog_views.catalog_management_productcatalog cc on aa.product_id = cc.id
where upper(trim(bb.source_code)) like '%B2B%'  AND UPPER(TRIM(bb.source_code))  not in ("B2BCRJ","B2BCMP","B2BCMH",  "B2BCUP","B2BCGJ","B2B-IGNORE"))
-- and source_code in ( "B2BCT","B2BCG")
)b
on a.state=b.state and a.item_sku=b.sku_code
where cms_Active="active"
) a
inner join (select Partner_id_,Facility from (SELECT

APL.transport_id AS Transporter_id,
     
      ATL.name AS Transporter_Name,

      APLFT.facility_id AS Facility,
     
      APLFT.pickuplocation_id AS Pickup_Location_ID,
     

      DF.id AS Franchise_ID,    
      DF.user_info_id AS Partner_ID,
      SUBSTR(DF.user_info_id,6) As Partner_id_,
     
      TRIM(INITCAP(CONCAT(AFR.first_name , ' ', AFR.last_name))) AS Partner_Name,

       ASO.Line_1 AS Address_Line_1,
     
      ASO.Line_2 AS Address_Line_2,
     
     
     
      APL.state AS State,
     
      APL.village AS Village,
     
      CASE WHEN DF.user_info_id LIKE '%sathi%' THEN INITCAP(REPLACE((CASE WHEN ASO.Line_2 LIKE'%|%' THEN TRIM(LOWER(SPLIT(ASO.Line_2,'|')[SAFE_OFFSET(2)]))
      WHEN ASO.Line_2 LIKE'%-%' THEN TRIM(LOWER(SPLIT(ASO.Line_2,'-')[SAFE_OFFSET(2)])) END),'*',''))
      ELSE APL.taluka END AS Taluka,
     
      CASE WHEN DF.user_info_id LIKE '%sathi%' THEN INITCAP(TRIM(ASO.shipping_address_city))
      ELSE APL.district END AS District,
     
      CASE WHEN DF.user_info_id LIKE '%sathi%' THEN CAST(ASO.shipping_address_pincode AS STRING)
      ELSE APL.pincode END AS PIN_Code,
     
      CASE WHEN DF.is_staff = 0 THEN 'LP' ELSE 'FO OR SAATHI' END AS Partner_Type_1,
     
     
     
     
      APLFT.line_haul_time AS Line_Haul_Time,
     
      APLFT.buffer_time AS Buffer_Time,
     
      (CAST(APLFT.line_haul_time AS NUMERIC) + CAST(APLFT.buffer_time AS NUMERIC)) AS Total_Transit_Time,
     
      CASE WHEN DF.user_info_id LIKE '%sathi%' THEN 'Sathi Partner' ELSE 'Partner' END AS Partner_Type,
     
     

      APL.door_delivery_cost,

      DF.is_active AS Active_Status,
     
      ASO.Partner_name,
      ASO.Farmer_id
     
     

FROM
      `agrostar-data.prod_agroex_db_views.assignment_franchise` AS AFR

     
      LEFT JOIN (SELECT
                      franchise_id,
                      pickuplocation_id
                 FROM
                      `agrostar-data.prod_agroex_db_views.assignment_pickuplocationfranchisemapping`
                 WHERE
                      id >= 1
                ) AS APFM ON AFR.id = APFM.franchise_id
     
      LEFT JOIN (SELECT
                      id,
                      transport_id,
                      is_active,
                      state,
                      village,
                      taluka,
                      district,
                      pincode,door_delivery_cost
                 FROM
                      `agrostar-data.prod_agroex_db_views.assignment_pickuplocation`
                 ) AS APL ON APFM.pickuplocation_id = APL.id

      LEFT JOIN (SELECT
                      id,
                      pickuplocation_id,
                      is_active,
                      facility_id,
                      line_haul_time,
                      buffer_time
                 FROM
                      `agrostar-data.prod_agroex_db_views.assignment_pickuplocationfacilitytat`
                ) AS APLFT ON APL.id = APLFT.pickuplocation_id

      LEFT JOIN (SELECT
                      id,
                      name,
                 FROM
                      `agrostar-data.prod_agroex_db_views.assignment_transport`
                ) AS ATL ON APL.transport_id = ATL.id
     
      LEFT JOIN (SELECT
                      id,
                      user_info_id,
                      is_active,
                      is_staff,
                      reverse_facility_id
                FROM
                      `agrostar-data.prod_db_views.delivery_franchise`
                ) AS DF ON AFR.franchise_id = DF.id


      LEFT JOIN (SELECT
                      *
                 FROM
                      (SELECT
                            *,
                            ROW_NUMBER() OVER (PARTITION BY Farmer_ID) AS Row_Num
                       FROM
                            (SELECT
                                  DISTINCT(customer_code) AS Farmer_ID,
                                  shipping_address_line_1 AS Line_1,
                                  shipping_address_line_2 AS Line_2,
                                  shipping_address_city,
                                  shipping_address_pincode,
                                  shipping_address_name as Partner_Name
                             FROM
                                  `agrostar-data.dwh_views.agrostar_sale_order`
                             WHERE
                                  dispatch_date IS NOT NULL)
                             ) WHERE Row_Num = 1
                             ) AS ASO ON ASO.Farmer_ID = SAFE_CAST(RIGHT(DF.user_info_id ,7) AS INT64) AND LENGTH(Line_1) > 15
     
WHERE
  AFR.is_active = 1 AND
  APLFT.is_active = 1 AND
  APL.is_active = 1
  --APLFT.facility_id IN ('RJ01', 'RAJ01', 'NGP01', 'MH01', 'LKO01', 'JDH01', 'JBL01', 'IDR01','IDR02','GJ01', 'GAG01', 'AGR01') AND
  --APL.state NOT IN ('Chhattisgarh','Delhi','Haryana','Telangana state') and
  --Farmer_id=7366981

ORDER BY
      APLFT.facility_id = 'LKO01',
      APLFT.facility_id = 'AGR01',
      APLFT.facility_id = 'JBL01',
      APLFT.facility_id = 'IDR01',
      APLFT.facility_id = 'IDR02',
      APLFT.facility_id = 'GAG01',
      APLFT.facility_id = 'JDH01',
      APLFT.facility_id = 'RJ01',
      APLFT.facility_id = 'NGP01',
      APLFT.facility_id = 'MH01',
      APLFT.facility_id = 'RAJ01',
      APLFT.facility_id = 'GJ01'))b
      on safe_Cast(a.farmer_id as string)=b.Partner_id_) a
      inner join (select distinct location_code,item_no from
(select location_code,item_no,im.name ,vendor_lot_no,ile.crated_on,expiry_date,
count(ins.serial_no)good_inventory,im.moq
 from
agrostar-data.pristine_wms_views.item_serial_inventory ins
left join
(select serial_no,min(crated_on)crated_on
from
 pristine_wms_views.item_inventory_ledger group by 1) ile
 on ins.serial_no = ile.serial_no
left join
agrostar-data.pristine_wms_views.item_mst im
on ins.item_no = im.item_code
where is_used = 0 and location_code not in ('INTRANSIT')
group by 1,2,3,4,5,6,8
 order by 6 desc)
 order by 1)b
 on a.facility=b.location_code and a.item_sku=b.item_no
)

where rank <=20
) group by 1
) where max_pg >=10)

) as ord



left join



(
SELECT distinct  *
--farmerId, state,count(farmerId)
--state,skuCode

from (


select farmer_Id as farmerId,name as productName,item_sku as skuCode,qty as quantity, '' as source,
 uuid  as appliedOffer,state
  from

(
SELECT *,

from

(
  select *except(facility,total_revenue,source),

from

(select a.*, rank()over(partition by a.farmer_id order by a.total_revenue desc) as rank from (select a.*,b.facility from (select a.*except(source),b.source_code as source from(select a.* from(select a.*,b.*except(state,cartCriteria_constraints_constraintList_params_itemQuanList_skuCode) from(select *except(item_Code)

from
( select a.* from

(select a.* from(select a.*,b.product_group,b.item_sku,b.total_revenue from(select distinct farmer_id,state,territory,revised_district from `offline_team.okr_data_live` where status='ACTIVE')
a

inner join
(
----districts top products ----
select distinct *except(rank) from (select a.*,rank()over(partition by a.state,territory,revised_district,product_group order by total_Revenue desc) as rank from(select state,territory,revised_district,product_group,item_Sku,round(sum(total_revenue))total_revenue from
(


------district top products apr may-----
select distinct state, territory,revised_district,product_group,item_sku,sum(total_Revenue)total_Revenue from(
select a.*,b.* from
(select a.*,b.*except(order_id) from(select owner_id,sales_order_id, from `prod_db_views.order_management_order` where  unicommerce_status not in ('APP_UNVERIFIED', 'CANCELLED','FUTURE ORDER', 'MOB_APP_UNVERIFIED','RECEIVED','RETURNED','PICKUPSCHEDULED','ERROR','PENDING') and order_type !="RETURN_ORDER" 
AND 
 DATE(created_on) BETWEEN 
  DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH), INTERVAL 0 MONTH)
AND 
  LAST_DAY(DATE_ADD(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH), INTERVAL 1 MONTH))
--DATE(created_on)between '2024-04-01' and '2024-05-31' 

AND LEFT(SOURCE,3)="B2B"
              ) a
              inner join
(select order_id,item_Sku,product_group,sum(total_price)total_revenue from `prod_db_views.order_management_orderitem` oi
left join `revenue_and_growth_team.sku_cat_repo` as cr on cr.item_sku_code = oi.Item_SKU
 where 
  DATE(created_on) BETWEEN 
  DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH), INTERVAL 0 MONTH)
AND 
  LAST_DAY(DATE_ADD(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH), INTERVAL 1 MONTH))
 --date(created_on) between '2024-04-01' and '2024-05-31'
  and product_group!="PSP" and  upper(cr.item_sku_code) not like "%MKT%" and lower(cr.category) not in ("hw","kit","seeds","ah","elec") and lower(Product_group) not like("%others%")and pl_NPL="PL"
  group by 1,2,3)b
  on a.sales_order_id=b.order_id) a  
  inner join (select farmer_id,state,territory,revised_district from `offline_team.okr_data_live`)b
  on a.owner_id=b.farmer_id)
  group by 1,2,3,4,5

  union all

  ------district top products last 45 days -----
select distinct state, territory,revised_district,product_group,item_sku,sum(total_Revenue)total_Revenue from(
select a.*,b.* from
(select a.*,b.*except(order_id) from
(select owner_id,sales_order_id, from `prod_db_views.order_management_order` where  unicommerce_status not in ('APP_UNVERIFIED', 'CANCELLED','FUTURE ORDER', 'MOB_APP_UNVERIFIED','RECEIVED','RETURNED','PICKUPSCHEDULED','ERROR','PENDING') and order_type !="RETURN_ORDER" 
AND date(created_on) between date_sub(current_date(), interval 45 day) and current_date()
--DATE(created_on)between '2025-03-10' and '2025-04-24' 
AND LEFT(SOURCE,3)="B2B"
) a
              inner join
 (select order_id,item_Sku,product_group,sum(total_price)total_revenue from `prod_db_views.order_management_orderitem` oi
left join `revenue_and_growth_team.sku_cat_repo` as cr on cr.item_sku_code = oi.Item_SKU
 where
 date(created_on) between date_sub(current_date(), interval 45 day) and current_date() 
 --date(created_on) between '2025-03-10' and '2025-04-24'
  and product_group!="PSP" and  upper(cr.item_sku_code) not like "%MKT%" and lower(cr.category) not in ("hw","kit","seeds","ah","elec") and lower(Product_group) not like("%others%") and pl_NPL="PL"
  group by 1,2,3)b
  on a.sales_order_id=b.order_id) a  
  inner join (select farmer_id,state,territory,revised_district from `offline_team.okr_data_live`)b
  on a.owner_id=b.farmer_id)
  group by 1,2,3,4,5
)group by 1,2,3,4,5) a )where rank =1

)b
on  a.territory=b.territory and a.revised_district=b.revised_district) a

------partner purchased products last 60 days-----

left join
(
select distinct farmer_id ,product_group,
from
(
select a.*,b.* from
(select a.*,b.*except(order_id)
from
(select owner_id,sales_order_id, from `prod_db_views.order_management_order` where  unicommerce_status not in ('APP_UNVERIFIED', 'CANCELLED','FUTURE ORDER', 'MOB_APP_UNVERIFIED','RECEIVED','RETURNED','PICKUPSCHEDULED','ERROR','PENDING') and order_type !="RETURN_ORDER" 
AND date(created_on) between date_sub(current_date(), interval 60 day) and current_date()
--DATE(created_on)between '2025-02-23' and '2025-04-24' 
AND LEFT(SOURCE,3)="B2B"

) a
inner join
 (select order_id,item_Sku,product_group,sum(total_price)total_revenue from `prod_db_views.order_management_orderitem` oi
left join `revenue_and_growth_team.sku_cat_repo` as cr on cr.item_sku_code = oi.Item_SKU
 where date(created_on) between date_sub(current_date(), interval 60 day) and current_date()
 --date(created_on) between '2025-02-23' and '2025-04-24'
  and product_group!="PSP" and  upper(cr.item_sku_code) not like "%MKT%" and lower(cr.category) not in ("hw","kit","seeds","ah","elec")
  and pl_NPL="PL"
  group by 1,2,3
  )b
  on a.sales_order_id=b.order_id) a  
  inner join (select farmer_id,state,territory,revised_district from `offline_team.okr_data_live`)b
  on a.owner_id=b.farmer_id
  )
)b
on a.farmer_id =b.farmer_id and a.product_group=b.product_Group
where b.farmer_id is null

)  a
-- WHERE NOT (
--     (product_group IN ("Amaze - X", "Rapigen", "TeBull", "Stelar", "Shutter") AND state = "MP") OR
--     (product_group IN ("Florofix", "TMT 70", "Kleenweed", "Waaris") AND state = "MH") OR
--     (product_group IN ("Metalgro", "Pure Kelp", "Shutter", "Sulphur Maxx") AND state = "UP") OR
--     (product_group IN ("Roztam", "Rapigen", "Metalgro", "Kill X", "Amaze - X") AND state = "BH") OR
--     (product_group IN ("Florence", "Roztam", "Kill X", "Rapigen", "Nutripro") AND state = "GJ") OR
--     (product_group IN ("Faster", "Kill X", "Roztam", "Dragnet", "TMT 70", "Rule") AND state = "RJ")
-- )

)a
left join (SELECT  distinct item_code,name,moq  as qty from pristine_wms_views.item_mst)b
    on a.item_sku=b.item_Code
)a
left join (SELECT
  uuid,
  cartCriteria_constraints_constraintList_params_itemQuanList_skuCode,right(source,2) as state,
  source
FROM (
  SELECT
    uuid,
    cartCriteria_constraints_constraintList_params_itemQuanList_skuCode,
    SPLIT(REPLACE(REPLACE(sources, '[', ''), ']', ''), ',') AS source_array
  FROM
    agrostar-data.prod_db_views.promotion
  WHERE
    LOWER(sources) LIKE "%b2b%"
    AND status IN ("ACTIVATED")
    -- and date(createdon)='2024-12-12'
    AND DATE(validTo) > CURRENT_DATE() - 1
    AND type = "OFFER"
)
CROSS JOIN UNNEST(source_array) AS source)b
on a.state=b.state and a.item_sku=b.cartCriteria_constraints_constraintList_params_itemQuanList_skuCode

)
a
left join
(SELECT DISTINCT
  CASE
    WHEN RIGHT(source, 2) = 'TS' THEN 'TG'
    WHEN RIGHT(source, 2) = 'CT' THEN 'CG'
    WHEN RIGHT(source, 2) = 'KA' THEN 'KR'
    ELSE RIGHT(source, 2)
  END AS state,
  skuCode,

FROM agrostar-data.prod_db_views.blockedSKUs
where date(uploadTime)='2024-01-08'                 ----------WHY_-------WHY-----WHY
)b
     on a.state=b.state and a.item_sku=b.skucode
     where b.state is null) a
     left join
     (
      SELECT DISTINCT
  CASE
    WHEN state = 'TS' THEN 'TG'
    WHEN state = 'CT' THEN 'CG'
    WHEN state = 'KA' THEN 'KR'
    ELSE state
  END AS state,*except(state)
      from
      (select aa.product_id, cc.sku_code, bb.source_code, case when source_code not in ("B2B", "B2B-IGNORE") Then Right(source_code,2) ELSE null end  as state,
case when aa.is_enabled = 1 then 'active' else 'inactive' end as cms_active,


from catalog_views.catalog_management_productsourceassociation aa
left join catalog_views.catalog_management_source bb on aa.applicable_source_id = bb.id
left join catalog_views.catalog_management_productcatalog cc on aa.product_id = cc.id
where upper(trim(bb.source_code)) like '%B2B%'  AND UPPER(TRIM(bb.source_code))  not in ("B2BCRJ","B2BCMP","B2BCMH",  "B2BCUP","B2BCGJ","B2B-IGNORE"))
-- and source_code in ( "B2BCT","B2BCG")
)b
on a.state=b.state and a.item_sku=b.sku_code
where cms_Active="active"
) a
inner join (select Partner_id_,Facility from (SELECT

APL.transport_id AS Transporter_id,
     
      ATL.name AS Transporter_Name,

      APLFT.facility_id AS Facility,
     
      APLFT.pickuplocation_id AS Pickup_Location_ID,
     

      DF.id AS Franchise_ID,    
      DF.user_info_id AS Partner_ID,
      SUBSTR(DF.user_info_id,6) As Partner_id_,
     
      TRIM(INITCAP(CONCAT(AFR.first_name , ' ', AFR.last_name))) AS Partner_Name,

       ASO.Line_1 AS Address_Line_1,
     
      ASO.Line_2 AS Address_Line_2,
     
     
     
      APL.state AS State,
     
      APL.village AS Village,
     
      CASE WHEN DF.user_info_id LIKE '%sathi%' THEN INITCAP(REPLACE((CASE WHEN ASO.Line_2 LIKE'%|%' THEN TRIM(LOWER(SPLIT(ASO.Line_2,'|')[SAFE_OFFSET(2)]))
      WHEN ASO.Line_2 LIKE'%-%' THEN TRIM(LOWER(SPLIT(ASO.Line_2,'-')[SAFE_OFFSET(2)])) END),'*',''))
      ELSE APL.taluka END AS Taluka,
     
      CASE WHEN DF.user_info_id LIKE '%sathi%' THEN INITCAP(TRIM(ASO.shipping_address_city))
      ELSE APL.district END AS District,
     
      CASE WHEN DF.user_info_id LIKE '%sathi%' THEN CAST(ASO.shipping_address_pincode AS STRING)
      ELSE APL.pincode END AS PIN_Code,
     
      CASE WHEN DF.is_staff = 0 THEN 'LP' ELSE 'FO OR SAATHI' END AS Partner_Type_1,
     
     
     
     
      APLFT.line_haul_time AS Line_Haul_Time,
     
      APLFT.buffer_time AS Buffer_Time,
     
      (CAST(APLFT.line_haul_time AS NUMERIC) + CAST(APLFT.buffer_time AS NUMERIC)) AS Total_Transit_Time,
     
      CASE WHEN DF.user_info_id LIKE '%sathi%' THEN 'Sathi Partner' ELSE 'Partner' END AS Partner_Type,
     
     

      APL.door_delivery_cost,

      DF.is_active AS Active_Status,
     
      ASO.Partner_name,
      ASO.Farmer_id
     
     

FROM
      `agrostar-data.prod_agroex_db_views.assignment_franchise` AS AFR

     
      LEFT JOIN (SELECT
                      franchise_id,
                      pickuplocation_id
                 FROM
                      `agrostar-data.prod_agroex_db_views.assignment_pickuplocationfranchisemapping`
                 WHERE
                      id >= 1
                ) AS APFM ON AFR.id = APFM.franchise_id
     
      LEFT JOIN (SELECT
                      id,
                      transport_id,
                      is_active,
                      state,
                      village,
                      taluka,
                      district,
                      pincode,door_delivery_cost
                 FROM
                      `agrostar-data.prod_agroex_db_views.assignment_pickuplocation`
                 ) AS APL ON APFM.pickuplocation_id = APL.id

      LEFT JOIN (SELECT
                      id,
                      pickuplocation_id,
                      is_active,
                      facility_id,
                      line_haul_time,
                      buffer_time
                 FROM
                      `agrostar-data.prod_agroex_db_views.assignment_pickuplocationfacilitytat`
                ) AS APLFT ON APL.id = APLFT.pickuplocation_id

      LEFT JOIN (SELECT
                      id,
                      name,
                 FROM
                      `agrostar-data.prod_agroex_db_views.assignment_transport`
                ) AS ATL ON APL.transport_id = ATL.id
     
      LEFT JOIN (SELECT
                      id,
                      user_info_id,
                      is_active,
                      is_staff,
                      reverse_facility_id
                FROM
                      `agrostar-data.prod_db_views.delivery_franchise`
                ) AS DF ON AFR.franchise_id = DF.id


      LEFT JOIN (SELECT
                      *
                 FROM
                      (SELECT
                            *,
                            ROW_NUMBER() OVER (PARTITION BY Farmer_ID) AS Row_Num
                       FROM
                            (SELECT
                                  DISTINCT(customer_code) AS Farmer_ID,
                                  shipping_address_line_1 AS Line_1,
                                  shipping_address_line_2 AS Line_2,
                                  shipping_address_city,
                                  shipping_address_pincode,
                                  shipping_address_name as Partner_Name
                             FROM
                                  `agrostar-data.dwh_views.agrostar_sale_order`
                             WHERE
                                  dispatch_date IS NOT NULL)
                             ) WHERE Row_Num = 1
                             ) AS ASO ON ASO.Farmer_ID = SAFE_CAST(RIGHT(DF.user_info_id ,7) AS INT64) AND LENGTH(Line_1) > 15
     
WHERE
  AFR.is_active = 1 AND
  APLFT.is_active = 1 AND
  APL.is_active = 1
  --APLFT.facility_id IN ('RJ01', 'RAJ01', 'NGP01', 'MH01', 'LKO01', 'JDH01', 'JBL01', 'IDR01','IDR02','GJ01', 'GAG01', 'AGR01') AND
  --APL.state NOT IN ('Chhattisgarh','Delhi','Haryana','Telangana state') and
  --Farmer_id=7366981

ORDER BY
      APLFT.facility_id = 'LKO01',
      APLFT.facility_id = 'AGR01',
      APLFT.facility_id = 'JBL01',
      APLFT.facility_id = 'IDR01',
      APLFT.facility_id = 'IDR02',
      APLFT.facility_id = 'GAG01',
      APLFT.facility_id = 'JDH01',
      APLFT.facility_id = 'RJ01',
      APLFT.facility_id = 'NGP01',
      APLFT.facility_id = 'MH01',
      APLFT.facility_id = 'RAJ01',
      APLFT.facility_id = 'GJ01'))b
      on safe_Cast(a.farmer_id as string)=b.Partner_id_) a
      inner join (select distinct location_code,item_no from
(select location_code,item_no,im.name ,vendor_lot_no,ile.crated_on,expiry_date,
count(ins.serial_no)good_inventory,im.moq
 from
agrostar-data.pristine_wms_views.item_serial_inventory ins
left join
(select serial_no,min(crated_on)crated_on
from
 pristine_wms_views.item_inventory_ledger group by 1) ile
 on ins.serial_no = ile.serial_no
left join
agrostar-data.pristine_wms_views.item_mst im
on ins.item_no = im.item_code
where is_used = 0 and location_code not in ('INTRANSIT')
group by 1,2,3,4,5,6,8
 order by 6 desc)
 order by 1)b
 on a.facility=b.location_code and a.item_sku=b.item_no
)

where rank <=20
 
)

 
)

)
--group by 1,2
order by 1
) as logic
 on ord.reference_customer_id=logic.farmerid



union all
(
  Select *

from
(

Select * from
(

SELECT San.reference_customer_id,san.name,san.partner_name,san.status,san.created_on,san.address_district,san.address_state,san.totalCreditLimit,
  san.businessCategory,
dip.order_month,dip.Revenue

From


(SELECT reference_customer_id,name,partner_name,status,contacts_mobile_number,DATE(created_on) AS created_on,address_district,address_state,totalCreditLimit,
  businessCategory
FROM replica_galaxy_views.institution
--FROM galaxy_views.institution

WHERE status = 'ACTIVE'
AND archive = FALSE
AND LOWER(ancestor_institutions_name) LIKE '%sathi%'
AND date(created_on) >= '2024-01-01'                 ----------WHY_-------WHY-----WHY
And name NOT LIKE '%Testing%'


 ) As San

LEFT JOIN (

  SELECT DISTINCT owner_id,State_,order_month,Sum(Sku_Revenue) As Revenue

From

(SELECT
T2.owner_id,
Right(T2.initiating_source,2) As State_,
date(T2.confirmed_on) As order_date,
date_trunc(date(T2.confirmed_on),month) order_month,
-- T1.quantity As Sku_Qyt,
T1.Total_Price As Sku_Revenue,      
T2.Unicommerce_status,
       
from `prod_db_views.order_management_orderitem` as T1

left join (Select distinct sales_order_id,unicommerce_id, initiating_source,
owner_id, channel,unicommerce_status,confirmed_on,order_type from `prod_db_views.order_management_order`) as T2

on T1.order_id = T2.sales_order_id
   
where T2.confirmed_on >= '2024-01-01'                 ----------WHY_-------WHY-----WHY
and  T2.unicommerce_status not in ('APP_UNVERIFIED', 'CANCELLED','FUTURE ORDER', 'MOB_APP_UNVERIFIED','RECEIVED','RETURNED','PICKUPSCHEDULED','ERROR','PENDING','WAITING_FOR_APPROVAL','ONLINE_PAYMENT_PENDING')
and T2.initiating_source Like "%B2B%"
-- and T1.item_sku = 'AGS-HW-1480'
and T2.order_type not in ('RETURNED_ORDER'))

group by 1,2,3

) as dip

On san.reference_customer_id = dip.owner_id)

where Revenue IS NULL
and safe_cast(REGEXP_REPLACE(totalCreditLimit, r'\..*', '') as int64) >= 10
 
and  reference_customer_id not in
(SELECT  farmer_id

from
(
SELECT farmer_id,
max(rank) as max_pg

from


(select *except(facility,total_revenue,source),

from

(select a.*, rank()over(partition by a.farmer_id order by a.total_revenue desc) as rank from (select a.*,b.facility from (select a.*except(source),b.source_code as source from(select a.* from(select a.*,b.*except(state,cartCriteria_constraints_constraintList_params_itemQuanList_skuCode) from(select *except(item_Code)

from
( select a.* from

(select a.* from(select a.*,b.product_group,b.item_sku,b.total_revenue from(select distinct farmer_id,state,territory,revised_district from `offline_team.okr_data_live` where status='ACTIVE')
a

inner join
(
----districts top products ----
select distinct *except(rank) from (select a.*,rank()over(partition by a.state,territory,revised_district,product_group order by total_Revenue desc) as rank from(select state,territory,revised_district,product_group,item_Sku,round(sum(total_revenue))total_revenue from
(


------district top products apr may-----
select distinct state, territory,revised_district,product_group,item_sku,sum(total_Revenue)total_Revenue from(
select a.*,b.* from
(select a.*,b.*except(order_id) from(select owner_id,sales_order_id, from `prod_db_views.order_management_order` where  unicommerce_status not in ('APP_UNVERIFIED', 'CANCELLED','FUTURE ORDER', 'MOB_APP_UNVERIFIED','RECEIVED','RETURNED','PICKUPSCHEDULED','ERROR','PENDING') and order_type !="RETURN_ORDER" 
AND 
DATE(created_on) BETWEEN 
  DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH), INTERVAL 0 MONTH)
AND 
  LAST_DAY(DATE_ADD(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH), INTERVAL 1 MONTH))

--DATE(created_on)between '2024-04-01' and '2024-05-31' 
AND LEFT(SOURCE,3)="B2B"
              ) a
              inner join
 (select order_id,item_Sku,product_group,sum(total_price)total_revenue from `prod_db_views.order_management_orderitem` oi
left join `revenue_and_growth_team.sku_cat_repo` as cr on cr.item_sku_code = oi.Item_SKU
 where 
 DATE(created_on) BETWEEN 
  DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH), INTERVAL 0 MONTH)
AND 
  LAST_DAY(DATE_ADD(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH), INTERVAL 1 MONTH))

 --date(created_on) between '2024-04-01' and '2024-05-31'
  and product_group!="PSP" and  upper(cr.item_sku_code) not like "%MKT%" and lower(cr.category) not in ("hw","kit","seeds","ah","elec") and lower(Product_group) not like("%others%")and pl_NPL="PL"
  group by 1,2,3)b
  on a.sales_order_id=b.order_id) a  
  inner join (select farmer_id,state,territory,revised_district from `offline_team.okr_data_live`)b
  on a.owner_id=b.farmer_id)
  group by 1,2,3,4,5

  union all

  ------district top products last 45 days -----
select distinct state, territory,revised_district,product_group,item_sku,sum(total_Revenue)total_Revenue from(
select a.*,b.* from
(select a.*,b.*except(order_id) from
(select owner_id,sales_order_id, from `prod_db_views.order_management_order` where  unicommerce_status not in ('APP_UNVERIFIED', 'CANCELLED','FUTURE ORDER', 'MOB_APP_UNVERIFIED','RECEIVED','RETURNED','PICKUPSCHEDULED','ERROR','PENDING') and order_type !="RETURN_ORDER" AND
 date(created_on) between date_sub(current_date(), interval 45 day) and current_date()
 --DATE(created_on) between '2025-03-10' and '2025-04-24' 
 AND LEFT(SOURCE,3)="B2B"
) a
              inner join
 (select order_id,item_Sku,product_group,sum(total_price)total_revenue from `prod_db_views.order_management_orderitem` oi
left join `revenue_and_growth_team.sku_cat_repo` as cr on cr.item_sku_code = oi.Item_SKU
 where
 date(created_on) between date_sub(current_date(), interval 45 day) and current_date()
 --date(created_on) between '2025-03-10' and '2025-04-24'
  and product_group!="PSP" and  upper(cr.item_sku_code) not like "%MKT%" and lower(cr.category) not in ("hw","kit","seeds","ah","elec") and lower(Product_group) not like("%others%") and pl_NPL="PL"
  group by 1,2,3)b
  on a.sales_order_id=b.order_id) a  
  inner join (select farmer_id,state,territory,revised_district from `offline_team.okr_data_live`)b
  on a.owner_id=b.farmer_id)
  group by 1,2,3,4,5
)group by 1,2,3,4,5) a )where rank =1

)b
on  a.territory=b.territory and a.revised_district=b.revised_district) a

------partner purchased products last 60 days-----

left join
(
select distinct farmer_id ,product_group,
from
(
select a.*,b.* from
(select a.*,b.*except(order_id)
from
(select owner_id,sales_order_id, from `prod_db_views.order_management_order` where  unicommerce_status not in ('APP_UNVERIFIED', 'CANCELLED','FUTURE ORDER', 'MOB_APP_UNVERIFIED','RECEIVED','RETURNED','PICKUPSCHEDULED','ERROR','PENDING') and order_type !="RETURN_ORDER" AND
date(created_on) between date_sub(current_date(), interval 60 day) and current_date()
  --DATE(created_on) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND CURRENT_DATE()
--or
 --DATE(created_on)between '2025-02-23' and '2025-04-24' 
 AND LEFT(SOURCE,3)="B2B"

) a
inner join
 (select order_id,item_Sku,product_group,sum(total_price)total_revenue from `prod_db_views.order_management_orderitem` oi
left join `revenue_and_growth_team.sku_cat_repo` as cr on cr.item_sku_code = oi.Item_SKU
 where date(created_on) between date_sub(current_date(), interval 60 day) and current_date()

 --DATE(created_on) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND CURRENT_DATE()
--or
 --date(created_on) between '2025-02-23' and '2025-04-24'
  and product_group!="PSP" and  upper(cr.item_sku_code) not like "%MKT%" and lower(cr.category) not in ("hw","kit","seeds","ah","elec")
  and pl_NPL="PL"
  group by 1,2,3
  )b
  on a.sales_order_id=b.order_id) a  
  inner join (select farmer_id,state,territory,revised_district from `offline_team.okr_data_live`)b
  on a.owner_id=b.farmer_id
  )
)b
on a.farmer_id =b.farmer_id and a.product_group=b.product_Group
where b.farmer_id is null

)  a
-- WHERE NOT (
--     (product_group IN ("Amaze - X", "Rapigen", "TeBull", "Stelar", "Shutter") AND state = "MP") OR
--     (product_group IN ("Florofix", "TMT 70", "Kleenweed", "Waaris") AND state = "MH") OR
--     (product_group IN ("Metalgro", "Pure Kelp", "Shutter", "Sulphur Maxx") AND state = "UP") OR
--     (product_group IN ("Roztam", "Rapigen", "Metalgro", "Kill X", "Amaze - X") AND state = "BH") OR
--     (product_group IN ("Florence", "Roztam", "Kill X", "Rapigen", "Nutripro") AND state = "GJ") OR
--     (product_group IN ("Faster", "Kill X", "Roztam", "Dragnet", "TMT 70", "Rule") AND state = "RJ")
-- )

)a
left join (SELECT  distinct item_code,name,moq  as qty from pristine_wms_views.item_mst)b
    on a.item_sku=b.item_Code
)a
left join (SELECT
  uuid,
  cartCriteria_constraints_constraintList_params_itemQuanList_skuCode,right(source,2) as state,
  source
FROM (
  SELECT
    uuid,
    cartCriteria_constraints_constraintList_params_itemQuanList_skuCode,
    SPLIT(REPLACE(REPLACE(sources, '[', ''), ']', ''), ',') AS source_array
  FROM
    agrostar-data.prod_db_views.promotion
  WHERE
    LOWER(sources) LIKE "%b2b%"
    AND status IN ("ACTIVATED")
    -- and date(createdon)='2024-12-12'
    AND DATE(validTo) > CURRENT_DATE() - 1
    AND type = "OFFER"
)
CROSS JOIN UNNEST(source_array) AS source)b
on a.state=b.state and a.item_sku=b.cartCriteria_constraints_constraintList_params_itemQuanList_skuCode

)
a
left join
(SELECT DISTINCT
  CASE
    WHEN RIGHT(source, 2) = 'TS' THEN 'TG'
    WHEN RIGHT(source, 2) = 'CT' THEN 'CG'
    WHEN RIGHT(source, 2) = 'KA' THEN 'KR'
    ELSE RIGHT(source, 2)
  END AS state,
  skuCode,

FROM agrostar-data.prod_db_views.blockedSKUs
where date(uploadTime)='2024-01-08'             ---------------------------------------WHY--------WHY---------WHY------WHY
)b
     on a.state=b.state and a.item_sku=b.skucode
     where b.state is null) a
     left join
     (
      SELECT DISTINCT
  CASE
    WHEN state = 'TS' THEN 'TG'
    WHEN state = 'CT' THEN 'CG'
    WHEN state = 'KA' THEN 'KR'
    ELSE state
  END AS state,*except(state)
      from
      (select aa.product_id, cc.sku_code, bb.source_code, case when source_code not in ("B2B", "B2B-IGNORE") Then Right(source_code,2) ELSE null end  as state,
case when aa.is_enabled = 1 then 'active' else 'inactive' end as cms_active,


from catalog_views.catalog_management_productsourceassociation aa
left join catalog_views.catalog_management_source bb on aa.applicable_source_id = bb.id
left join catalog_views.catalog_management_productcatalog cc on aa.product_id = cc.id
where upper(trim(bb.source_code)) like '%B2B%'  AND UPPER(TRIM(bb.source_code))  not in ("B2BCRJ","B2BCMP","B2BCMH",  "B2BCUP","B2BCGJ","B2B-IGNORE"))
-- and source_code in ( "B2BCT","B2BCG")
)b
on a.state=b.state and a.item_sku=b.sku_code
where cms_Active="active"
) a
inner join (select Partner_id_,Facility from (SELECT

APL.transport_id AS Transporter_id,
     
      ATL.name AS Transporter_Name,

      APLFT.facility_id AS Facility,
     
      APLFT.pickuplocation_id AS Pickup_Location_ID,
     

      DF.id AS Franchise_ID,    
      DF.user_info_id AS Partner_ID,
      SUBSTR(DF.user_info_id,6) As Partner_id_,
     
      TRIM(INITCAP(CONCAT(AFR.first_name , ' ', AFR.last_name))) AS Partner_Name,

       ASO.Line_1 AS Address_Line_1,
     
      ASO.Line_2 AS Address_Line_2,
     
     
     
      APL.state AS State,
     
      APL.village AS Village,
     
      CASE WHEN DF.user_info_id LIKE '%sathi%' THEN INITCAP(REPLACE((CASE WHEN ASO.Line_2 LIKE'%|%' THEN TRIM(LOWER(SPLIT(ASO.Line_2,'|')[SAFE_OFFSET(2)]))
      WHEN ASO.Line_2 LIKE'%-%' THEN TRIM(LOWER(SPLIT(ASO.Line_2,'-')[SAFE_OFFSET(2)])) END),'*',''))
      ELSE APL.taluka END AS Taluka,
     
      CASE WHEN DF.user_info_id LIKE '%sathi%' THEN INITCAP(TRIM(ASO.shipping_address_city))
      ELSE APL.district END AS District,
     
      CASE WHEN DF.user_info_id LIKE '%sathi%' THEN CAST(ASO.shipping_address_pincode AS STRING)
      ELSE APL.pincode END AS PIN_Code,
     
      CASE WHEN DF.is_staff = 0 THEN 'LP' ELSE 'FO OR SAATHI' END AS Partner_Type_1,
     
     
     
     
      APLFT.line_haul_time AS Line_Haul_Time,
     
      APLFT.buffer_time AS Buffer_Time,
     
      (CAST(APLFT.line_haul_time AS NUMERIC) + CAST(APLFT.buffer_time AS NUMERIC)) AS Total_Transit_Time,
     
      CASE WHEN DF.user_info_id LIKE '%sathi%' THEN 'Sathi Partner' ELSE 'Partner' END AS Partner_Type,
     
     

      APL.door_delivery_cost,

      DF.is_active AS Active_Status,
     
      ASO.Partner_name,
      ASO.Farmer_id
     
     

FROM
      `agrostar-data.prod_agroex_db_views.assignment_franchise` AS AFR

     
      LEFT JOIN (SELECT
                      franchise_id,
                      pickuplocation_id
                 FROM
                      `agrostar-data.prod_agroex_db_views.assignment_pickuplocationfranchisemapping`
                 WHERE
                      id >= 1
                ) AS APFM ON AFR.id = APFM.franchise_id
     
      LEFT JOIN (SELECT
                      id,
                      transport_id,
                      is_active,
                      state,
                      village,
                      taluka,
                      district,
                      pincode,door_delivery_cost
                 FROM
                      `agrostar-data.prod_agroex_db_views.assignment_pickuplocation`
                 ) AS APL ON APFM.pickuplocation_id = APL.id

      LEFT JOIN (SELECT
                      id,
                      pickuplocation_id,
                      is_active,
                      facility_id,
                      line_haul_time,
                      buffer_time
                 FROM
                      `agrostar-data.prod_agroex_db_views.assignment_pickuplocationfacilitytat`
                ) AS APLFT ON APL.id = APLFT.pickuplocation_id

      LEFT JOIN (SELECT
                      id,
                      name,
                 FROM
                      `agrostar-data.prod_agroex_db_views.assignment_transport`
                ) AS ATL ON APL.transport_id = ATL.id
     
      LEFT JOIN (SELECT
                      id,
                      user_info_id,
                      is_active,
                      is_staff,
                      reverse_facility_id
                FROM
                      `agrostar-data.prod_db_views.delivery_franchise`
                ) AS DF ON AFR.franchise_id = DF.id


      LEFT JOIN (SELECT
                      *
                 FROM
                      (SELECT
                            *,
                            ROW_NUMBER() OVER (PARTITION BY Farmer_ID) AS Row_Num
                       FROM
                            (SELECT
                                  DISTINCT(customer_code) AS Farmer_ID,
                                  shipping_address_line_1 AS Line_1,
                                  shipping_address_line_2 AS Line_2,
                                  shipping_address_city,
                                  shipping_address_pincode,
                                  shipping_address_name as Partner_Name
                             FROM
                                  `agrostar-data.dwh_views.agrostar_sale_order`
                             WHERE
                                  dispatch_date IS NOT NULL)
                             ) WHERE Row_Num = 1
                             ) AS ASO ON ASO.Farmer_ID = SAFE_CAST(RIGHT(DF.user_info_id ,7) AS INT64) AND LENGTH(Line_1) > 15
     
WHERE
  AFR.is_active = 1 AND
  APLFT.is_active = 1 AND
  APL.is_active = 1
  --APLFT.facility_id IN ('RJ01', 'RAJ01', 'NGP01', 'MH01', 'LKO01', 'JDH01', 'JBL01', 'IDR01','IDR02','GJ01', 'GAG01', 'AGR01') AND
  --APL.state NOT IN ('Chhattisgarh','Delhi','Haryana','Telangana state') and
  --Farmer_id=7366981

ORDER BY
      APLFT.facility_id = 'LKO01',
      APLFT.facility_id = 'AGR01',
      APLFT.facility_id = 'JBL01',
      APLFT.facility_id = 'IDR01',
      APLFT.facility_id = 'IDR02',
      APLFT.facility_id = 'GAG01',
      APLFT.facility_id = 'JDH01',
      APLFT.facility_id = 'RJ01',
      APLFT.facility_id = 'NGP01',
      APLFT.facility_id = 'MH01',
      APLFT.facility_id = 'RAJ01',
      APLFT.facility_id = 'GJ01'))b
      on safe_Cast(a.farmer_id as string)=b.Partner_id_) a
      inner join (select distinct location_code,item_no from
(select location_code,item_no,im.name ,vendor_lot_no,ile.crated_on,expiry_date,
count(ins.serial_no)good_inventory,im.moq
 from
agrostar-data.pristine_wms_views.item_serial_inventory ins
left join
(select serial_no,min(crated_on)crated_on
from
 pristine_wms_views.item_inventory_ledger group by 1) ile
 on ins.serial_no = ile.serial_no
left join
agrostar-data.pristine_wms_views.item_mst im
on ins.item_no = im.item_code
where is_used = 0 and location_code not in ('INTRANSIT')
group by 1,2,3,4,5,6,8
 order by 6 desc)
 order by 1)b
 on a.facility=b.location_code and a.item_sku=b.item_no
)

where rank <=20
) group by 1
) where max_pg >=10)

) as ord


left join

(select farmer_Id as farmerId,item_type_name as productName,Sku_code as skuCode,MOQ as quantity, '' as source,
 uuid  as appliedOffer,state
  from

(
 select a.*,b.uuid, from ((
  SELECT
      a.*,
      b.*except(state)
  FROM
      (SELECT farmer_id, state, territory, revised_district
       FROM `offline_team.okr_data_live`
       WHERE status = "ACTIVE"
         AND state IS NOT NULL
         AND territory IS NOT NULL) a
  INNER JOIN
      (
          SELECT
              State,
              Product,
              Sku_code,
              item_type_name,
              MOQ
          FROM
UNNEST([
  STRUCT('UP' AS State, 'Bhumika' AS Product, 'AGS-CN-612' AS Sku_code, 'Bhumika (Organic Plant Vitaliser) 10 kg' AS item_type_name, 2 AS MOQ),
  ('UP', 'Battery Pump', 'AGS-HW-1591', 'Gladiator Plus Double Motor Pump GLDM1012 (12x12)', 1),
  ('UP', 'Sugarcane Special', 'AGS-CN-208', 'Sugarcane Special (Biostimulant) 500 g', 20),
  ('UP', 'Rapigen', 'AGS-CP-1446', 'Rapigen (Chlorantraniliprole 18.5% SC ) 150 ml', 20),
  ('UP', 'Sanchaar', 'AGS-CN-759', 'Sanchaar (Bio-Enriched Organic Manure) 10 kg', 2),
  ('UP', 'Green gram', 'AGS-S-4719', 'Agrostar 7444 Green Gram (2 Kg) Seeds', 20),
  ('UP', 'Rapigen', 'AGS-CP-1445', 'Rapigen (Chlorantraniliprole 18.5% SC ) 60 ml', 25),
  ('UP', 'Faster', 'AGS-CN-737', 'Faster (Biostimulant) 500ml', 8),
  ('UP', 'Power Gel', 'AGS-CN-004', 'Power Gel (Organic Plant Nutrient) 500 g', 8),
  ('UP', 'Rapigen GR', 'AGS-CP-1448', 'Rapigen GR (Chlorantraniliprole 0.4% GR) 4 kg', 5),
  ('UP', 'Battery Pump', 'AGS-HW-1589', 'Gladiator Plus Battery Pump GL108 (12x8)', 1),
  ('UP', 'Sanchaar', 'AGS-CN-818', 'Sanchaar (Bio-Enriched Organic Manure) 25 kg', 1),
  ('UP', 'Humic Power', 'AGS-CN-715', 'Humic Power 8X250 g (Humic & Fulvic Acid 50% min.) 2 kg Bucket', 4),
  ('UP', 'Tarplus', 'AGS-HW-708', 'Tarplus Sheet 15*11 (Tadpatri) Yellow Lemon', 1),
  ('UP', 'Torch', 'AGS-HW-1592', 'Commando Plus Rechargeable LED torch - CM007', 4),
  ('UP', 'Watermelon PL', 'AGS-S-4348', 'Agrostar Red Baby F1 Watermelon (50g)', 1),
  ('UP', 'Tarplus', 'AGS-HW-710', 'Tarplus Sheet 24*17 (Tadpatri) Yellow Lemon', 1),
  ('UP', 'Bhumika', 'AGS-CN-861', 'Bhumika(Organic Plant Vitaliser)15NX2kg Drum 30KG', 1),
  ('UP', 'Selzic', 'AGS-CN-643', 'Selzic (Sulphur 65% + Zinc 18% Granules) 3 kg', 4),
  ('UP', 'Arex 505', 'AGS-CP-937', 'Arex 505 (Chlorpyriphos 50% + Cypermethrin 5% EC) 1 litre', 6),
  ('UP', 'Cauliflower', 'AGS-S-4316', 'Agrostar Aditi F1 Cauliflower (10g)', 1),

--2. MP
('MP', 'Agrostar WSF', 'AGS-CN-983', 'NPK 10:20:20+Mgo+TE 5 kg', 4),
  ('MP', 'Agrostar WSF', 'AGS-CN-829', 'NPK 13:40:13 (TE) 25 kg', 1),
  ('MP', 'Agrostar WSF', 'AGS-CN-831', 'NPK 0:52:34 (MKP) 25 kg', 1),
  ('MP', 'Agrostar WSF', 'AGS-CN-828', 'NPK 12:61:0 (MAP) 25 kg', 1),

  ('MP', 'Bhumika', 'AGS-CN-612', 'Bhumika (Organic Plant Vitaliser) 10 kg', 2),
  ('MP', 'Florence', 'AGS-CN-777', 'Florence (Biostimulants) 500 ml', 10),
  ('MP', 'Florofix', 'AGS-CN-378', 'Florofix (Protein Hydrolysate Powder 50% TC) 250g', 25),
  ('MP', 'Hold On', 'AGS-CN-794', 'Hold On (Alpha Naphthyl Acetic Acid 4.5% SL) 100 ml', 40),
  ('MP', 'Hold On', 'AGS-CN-793', 'Hold On (Alpha Naphthyl Acetic Acid 4.5%SL) 500 ml', 8),
  ('MP', 'Humic Power NX', 'AGS-CN-713', 'Humic Power NX (Humic & Fulvic Derivative) 200g', 20),

  ('MP', 'Arex 505', 'AGS-CP-937', 'Arex 505 (Chlorpyriphos 50% + Cypermethrin 5% EC) 1 litre', 6),
  ('MP', 'Parashoot', 'AGS-CP-1488', 'Parashoot (Imazethapyr 10% SL) 500 ml', 12),
  ('MP', 'Perpendi Xtra', 'AGS-CP-1467', 'Agrostar Perpendi Xtra (Pendimethalin 38.7 % CS) 3.5 litre', 3),
  ('MP', 'Rapigen', 'AGS-CP-1444', 'Rapigen (Chlorantraniliprole 18.5% SC ) 30 ml', 50),
  ('MP', 'Amaze-X', 'AGS-CP-911', 'Amaze-X (Emamectin Benzoate 5% SG) 500 g', 8),

  ('MP', 'Maize PL', 'AGS-S-4133', 'Agrostar Hyb. 101 Maize (4 kg) Seeds', 8),
  ('MP', 'Hybrid Paddy PL', 'AGS-S-3232', 'Agrostar 4699 Gold Paddy (3 Kg) Seeds', 10),
  ('MP', 'Hybrid Paddy PL', 'AGS-S-4227', 'Agrostar 4799 Paddy (3 Kg) Seeds', 10),
  ('MP', 'Fodder PL', 'AGS-S-4465', 'Agrostar 6221 SSG Fodder (3 Kg) Seed', 15),
  ('MP', 'Cotton PL', 'AGS-S-3189', 'Agrostar Shivansh BG II Cotton Seed', 30),

--3. RJ
      ('RJ', 'Agrostar WSF', 'AGS-CN-833', 'Calcium Nitrate 25 kg', 1),
      ('RJ', 'Allstar', 'AGS-CN-269', 'AllStar 250 gm', 40),
      ('RJ', 'Bhumika', 'AGS-CN-443', 'Bhumika (Organic Plant Vitaliser) 4 kg Jar', 4),
      ('RJ', 'Faster', 'AGS-CN-880', 'Faster (Biostimulant) 100ml', 20),
      ('RJ', 'Florence', 'AGS-CN-777', 'Florence (Biostimulants) 500 ml', 10),

      ('RJ', 'Florofix', 'AGS-CN-378', 'Florofix (Protein Hydrolysate Powder 50% TC) 250g', 25),
      ('RJ', 'Hold On', 'AGS-CN-793', 'Hold On (Alpha Naphthyl Acetic Acid 4.5%SL) 500 ml', 8),
      ('RJ', 'Hold On', 'AGS-CN-794', 'Hold On (Alpha Naphthyl Acetic Acid 4.5% SL) 100 ml', 40),
      ('RJ', 'Humic Power NX', 'AGS-CN-713', 'Humic Power NX (Humic & Fulvic Derivative) 200g', 20),
      ('RJ', 'Humic Power NX', 'AGS-CN-864', 'Humic Power NX (Humic & Fulvic Derivative) 2kg Bucket', 4),

      ('RJ', 'Lihostar', 'AGS-CN-710', 'LihoStar (Chlormequat Chloride 50% SL) 1 litre', 6),
      ('RJ', 'Nanovita Boron', 'AGS-CN-815', 'Nanovita B10 (Boron Ethanolamine 10%) 250ml', 10),
      ('RJ', 'Nanovita Calcium', 'AGS-CN-771', 'Nanovita CA 11 (Gluconolactate Liquid Calcium 11%) 1 Ltr', 6),
      ('RJ', 'Nutripro Zinc HEDP', 'AGS-CN-877', 'Nutripro Zinc ( Zn 17% HEDP ) 500 g', 4),
      ('RJ', 'Peak Booster', 'AGS-CN-792', 'Peak Booster (Triacontanol 0.1% EW) 250 ml', 16),

      ('RJ', 'Power Gel', 'AGS-CN-004', 'Power Gel (Organic Plant Nutrient) 500 g', 8),
      ('RJ', 'Pure Kelp', 'AGS-CN-761', 'Pure Kelp (Seaweed Extract) 500 ml', 8),
      ('RJ', 'Sanchaar', 'AGS-CN-954', 'Sanchaar (Bio-Enriched Organic Manure) 5 kg', 10),
      ('RJ', 'Selzic', 'AGS-CN-643', 'Selzic (Sulphur 65% + Zinc 18% Granules) 3 kg', 4),
      ('RJ', 'Stelar', 'AGS-CN-309', 'Stelar (Giberellic Acid 0.001% L) 1 litre', 6),

--4. GJ
      ('GJ', 'Agrostar WSF', 'AGS-CN-820', 'NPK 19:19:19 (TE) 1 kg', 25),
      ('GJ', 'Power Gel', 'AGS-CN-004', 'Power Gel (Organic Plant Nutrient) 500 g', 8),
      ('GJ', 'Nutro Gel', 'AGS-CN-267', 'Nutro Gel (Organic Biostimulant) 150 g', 20),
      ('GJ', 'Allstar', 'AGS-CN-269', 'AllStar 250 gm', 40),
      ('GJ', 'GJ-Nutripro Grade 4', 'AGS-CN-301', 'NutriPro Grade 4 (Micronutrient Mixture-GJ) 250 g', 40),

      ('GJ', 'Wetsil Plus', 'AGS-CN-305', 'Wetsil Plus (Ethoxylated Trisilioxane Surfactant) 100 ml', 20),
      ('GJ', 'Bhumika', 'AGS-CN-443', 'Bhumika (Organic Plant Vitaliser) 4 kg Jar', 4),
      ('GJ', 'Florofix', 'AGS-CN-378', 'Florofix (Protein Hydrolysate Powder 50% TC) 250g', 25),
      ('GJ', 'Stelar', 'AGS-CN-308', 'Stelar (Giberellic Acid 0.001% L) 500 ml', 8),
      ('GJ', 'Sulphur 90G', 'AGS-CN-425', 'Sulphur 90G (Sulphur 90% Powder) 3 kg', 6),

      ('GJ', 'Humic Power NX', 'AGS-CN-713', 'Humic Power NX (Humic & Fulvic Derivative) 200g', 20),
      ('GJ', 'Faster', 'AGS-CN-737', 'Faster (Biostimulant) 500ml', 8),
      ('GJ', 'Selzic', 'AGS-CN-643', 'Selzic (Sulphur 65% + Zinc 18% Granules) 3 kg', 4),
      ('GJ', 'Sulphur Maxx', 'AGS-CN-702', 'Sulphur Maxx (Sulphur 90% Fertilizer) 30 kg Drum', 1),
      ('GJ', 'Zynx', 'AGS-CN-645', 'Zynx (Zinc Oxide Suspension 39.5% SC) 250 ml', 40),

      ('GJ', 'Silikon', 'AGS-CN-747', 'Silikon (Silicon Fertiliser) 250 ml', 40),
      ('GJ', 'NutriPro Magnesium Sulphate', 'AGS-CN-765', 'Nutri Pro Magnesium Sulphate 1 kg', 20),
      ('GJ', 'Pure Kelp', 'AGS-CN-761', 'Pure Kelp (Seaweed Extract) 500 ml', 8),
      ('GJ', 'Sanchaar', 'AGS-CN-759', 'Sanchaar (Bio-Enriched Organic Manure) 10 kg', 2),
      ('GJ', 'Nanovita Calcium', 'AGS-CN-771', 'Nanovita CA 11 (Gluconolactate Liquid Calcium 11%) 1 Ltr', 6),
 
--5. MH
      ('MH', 'Bhumika', 'AGS-CN-860', 'Bhumika (Organic Plant Vitaliser) 8N X 4kg Drum 32 kg', 1),
      ('MH', 'Wetsil Plus', 'AGS-CN-656', 'Wetsil Plus (Ethoxylated Trisilioxane Surfactant)250 ml', 8),
      ('MH', 'Sanchaar', 'AGS-CN-818', 'Sanchaar (Bio-Enriched Organic Manure) 25 kg', 1),
      ('MH', 'Stelar', 'AGS-CN-716', 'Stelar (Gibberellic Acid 0.001%) 100 ml', 20),
      ('MH', 'Lihostar', 'AGS-CN-712', 'LihoStar (Chlormequat Chloride 50% SL) 250 ml', 16),

      ('MH', 'Roztam', 'AGS-CP-1216', 'Roztam (Azoxystrobin 11% + Tebuconazole 18.3% SC) 1 litre', 10),
      ('MH', 'Agronil Gr', 'AGS-CP-1264', 'Agronil GR (Fipronil 0.3% GR) 1 kg', 25),
      ('MH', 'Kleenweed', 'AGS-CP-1144', 'Kleenweed (2-4-D Amine Salt 58% SL) 1 litre', 6),
      ('MH', 'Panaca M-45', 'AGS-CP-627', 'Panaca M-45 (Mancozeb 75% WP) 1 kg', 6),
      ('MH', 'Cruzer', 'AGS-CP-908', 'Cruzer (Thiamethoxam 25% WG) 1 kg', 6),

      ('MH', 'Sugarcane Special', 'AGS-CN-208', 'Sugarcane Special (Biostimulant) 500 g', 20),
      ('MH', 'Faster', 'AGS-CN-858', 'Faster (Biostimulant) 1 litre', 4),
      ('MH', 'Agrostar WSF', 'AGS-CN-822', 'NPK 13:40:13 (TE) 1 kg', 25),
      ('MH', 'Power Gel', 'AGS-CN-004', 'Power Gel (Organic Plant Nutrient) 500 g', 8),
      ('MH', 'MH-Nutripro Grade 2', 'AGS-CN-299', 'NutriPro Grade 2 (Micronutrient Mixture-MH) 250 g', 40),

      ('MH', 'Brinjal', 'AGS-S-4320', 'Agrostar Black Bell F1 Brinjal (1000 Seed)', 1),
      ('MH', 'Tarplus', 'AGS-HW-1244', 'Tarplus Sheet 40*30 (Tadpatri) Yellow Lemon', 1),
      ('MH', 'Okra PL', 'AGS-S-4347', 'Agrostar Janki F1 Okra (250g)', 1),
      ('MH', 'Torch', 'AGS-HW-1592', 'Commando Plus Rechargeable LED torch - CM007', 4),
      ('MH', 'Battery Pump', 'AGS-HW-774', 'Gladiator Battery Spray Pump (12*12) (ORANGE)', 1),

--6. KA => KR
      ('KR', 'Bhumika WSP', 'AGS-CN-951', 'Bhumika WSP 400 g', 20),
      ('KR', 'Faster', 'AGS-CN-738', 'Faster (Biostimulant) 250ml', 16),
      ('KR', 'Karbonik', 'AGS-CN-774', 'Karbonik C:N (Organic Carbon 14%) 1 litre', 10),
      ('KR', 'Bhumika', 'AGS-CN-730', 'Bhumika (Organic Plant Vitaliser) Drum 30 kg', 1),
      ('KR', 'Humic Power NX', 'AGS-CN-713', 'Humic Power NX (Humic & Fulvic Derivative) 200g', 20),

      ('KR', 'Agetate Gold', 'AGS-CP-1498', 'Agetate Gold (Acephate 50% + Imidacloprid 1.8% SP) 250g', 40),
      ('KR', 'Adonix', 'AGS-CP-1359', 'Adonix (Pyriproxyfen 5% + Diafenthiuron 25% SE) 500 ml', 8),
      ('KR', 'Agloro', 'AGS-CP-1717', 'Agloro (CHLORPYRIPHOS 20% EC) 250 ML', 40),
      ('KR', 'Agroar', 'AGS-CP-1257', 'Agroar (Diamethoate 30% EC) 500 ml', 8),
      ('KR', 'Adonix Neo', 'AGS-CP-1634', 'Adonix Neo (Pyriproxyfen 10% + Bifenthrin 10% EC) 100 ml', 100),

      ('KR', 'Peak Booster', 'AGS-CN-791', 'Peak Booster (Triacontanol 0.1% EW) 500 ml', 8),
      ('KR', 'Power Gel', 'AGS-CN-801', 'Power Gel (Organic Plant Nutrient) 40 g Sachet', 25),
      ('KR', 'Stelar', 'AGS-CN-308', 'Stelar (Giberellic Acid 0.001% L) 500 ml', 8),
      ('KR', 'Pure Kelp', 'AGS-CN-814', 'Pure Kelp (Seaweed Extract) 250 ml', 16),
      ('KR', 'Nanovita Calcium', 'AGS-CN-772', 'Nanovita CA 11(Gluconolactate Liquid Calcium 11%) 500 ml', 8),

      ('KR', 'Kopigo', 'AGS-CP-1535', 'Kopigo (Chlorantraniliprole 9.3% + Lambda 4.6% ZC) 80 ml', 10),
      ('KR', 'Kleenweed', 'AGS-CP-1145', 'Kleenweed (2-4-D Amine Salt 58% SL) 400 ml', 10),
      ('KR', 'Kill-X', 'AGS-CP-1504', 'Kill-X (Thiamethoxam 12.6% + Lambdacyhalothrin 9.5% ZC) 1 Litre', 6),
      ('KR', 'Lambada', 'AGS-CP-1747', 'Lambada (Lambda Cyhalothrin 4.9 % CS) 500 ml', 8),
      ('KR', 'Madrid', 'AGS-CP-598', 'Madrid (Acetamiprid 20% SP) 100 g', 20),

--7. AP => AD
      ('AD', 'Faster', 'AGS-CN-858', 'Faster (Biostimulant) 1 litre', 4),
      ('AD', 'Glyclean Power', 'AGS-CP-1555', 'Glyclean Power  (Glufosinate  Ammonium  13.5 SL) 1 litre', 10),
      ('AD', 'Amaze-X', 'AGS-CP-600', 'Amaze-X (Emamectin Benzoate 5% SG) 100 g', 20),
      ('AD', 'Faster', 'AGS-CN-737', 'Faster (Biostimulant) 500ml', 8),
      ('AD', 'Agrostar WSF', 'AGS-CN-820', 'NPK 19:19:19 (TE)1 kg', 25),

      ('AD', 'Paraquit', 'AGS-CP-1310', 'Paraquit (Paraquat Dichloride 24% SL) 1 litre', 6),
      ('AD', 'Adonix', 'AGS-CP-1359', 'Adonix (Pyriproxyfen 5% + Diafenthiuron 25% SE) 500 ml', 8),
      ('AD', 'Power Gel', 'AGS-CN-004', 'Power Gel (Organic Plant Nutrient) 500 g', 8),
      ('AD', 'Bhumika', 'AGS-CN-443', 'Bhumika (Organic Plant Vitaliser) 4 kg Jar', 4),
      ('AD', 'Humic Power NX', 'AGS-CN-713', 'Humic Power NX (Humic & Fulvic Derivative) 200g', 20),

      ('AD', 'Oxivia', 'AGS-CP-1143', 'Oxivia (Oxyflourfen 23.5% EC) 250 ml', 16),
      ('AD', 'Panaca M-45', 'AGS-CP-627', 'Panaca M-45 (Mancozeb 75% WP) 1 kg', 6),
      ('AD', 'Capasity', 'AGS-CP-1506', 'Capasity (Captan 70% + Hexaconazole 5% WP) 250 g', 16),
      ('AD', 'Paraquit', 'AGS-CP-1311', 'Paraquit (Paraquat Dichloride 24% SL) 5 litre', 2),
      ('AD', 'Glyclean Power', 'AGS-CP-1556', 'Glyclean Power  (Glufosinate  Ammonium  13.5 SL) 500 ml', 20),

      ('AD', 'Bhumika', 'AGS-CN-950', 'Bhumika (Organic Plant Vitaliser) 4kg pouch', 5),
      ('AD', 'Capasity', 'AGS-CP-1505', 'Capasity (Captan 70% + Hexaconazole 5% WP) 500 g', 8),
      ('AD', 'Peak Booster', 'AGS-CN-791', 'Peak Booster (Triacontanol 0.1% EW) 500 ml', 8),
      ('AD', 'Wetsil Plus', 'AGS-CN-902', 'Wetsil Plus (Ethoxylated Trisilioxane Surfactant) 500ml', 8),
      ('AD', 'Humic Power NX', 'AGS-CN-864', 'Humic Power NX (Humic & Fulvic Derivative) 2kg Bucket', 4),


--8. TS => TG
      ('TG', 'Agrostar WSF', 'AGS-CN-820', 'NPK 19:19:19 (TE)1 kg', 25),
      ('TG', 'Glyclean', 'AGS-CP-1218', 'Glyclean (Glyphosate 41 %) 5 litre', 2),
      ('TG', 'Glyclean', 'AGS-CP-1147', 'Glyclean (Glyphosate 41 % SL) 1 litre', 6),
      ('TG', 'Glyclean 71', 'AGS-CP-1219', 'Glyclean 71 (Glyphosate 71% SG) 100 g', 50),
      ('TG', 'Paraquit', 'AGS-CP-1310', 'Paraquit (Paraquat Dichloride 24% SL) 1 litre', 6),
      ('TG', 'Paraquit', 'AGS-CP-1344', 'Paraquit (Paraquit Dichloride 24% SL) 500 ml', 8),
      ('TG', 'Paraquit', 'AGS-CP-1311', 'Paraquit (Paraquat Dichloride 24% SL) 5 litre', 2),
      ('TG', 'Cotton PL', 'AGS-S-3189', 'Agrostar Shivansh BG II Cotton Seed', 30),
      ('TG', 'Cotton PL', 'AGS-S-4651', 'Agrostar EXPERT BG II Cotton Seeds', 30),
      ('TG', 'Cotton PL', 'AGS-S-4550', 'Agrostar Pearl BG II Cotton Seed', 30),

--9. CH => CG
      ('CG', 'Bhumika', 'AGS-CN-860', 'Bhumika (Organic Plant Vitaliser) 8N X 4kg Drum 32 kg', 1),
      ('CG', 'Faster', 'AGS-CN-738', 'Faster (Biostimulant) 250ml', 16),
      ('CG', 'Humic Power NX', 'AGS-CN-713', 'Humic Power NX (Humic & Fulvic Derivative) 200g', 20),
      ('CG', 'Nutripro Zinc HEDP', 'AGS-CN-875', 'Nutripro Zinc ( Zn 17% HEDP ) 100 g', 40),
      ('CG', 'Power Gel', 'AGS-CN-801', 'Power Gel (Organic Plant Nutrient) 40 g Sachet', 25),

      ('CG', 'Selzic', 'AGS-CN-643', 'Selzic (Sulphur 65% + Zinc 18% Granules) 3 kg', 4),
      ('CG', 'Stelar', 'AGS-CN-716', 'Stelar (Gibberellic Acid 0.001%) 100 ml', 20),
      ('CG', 'Wetsil Plus', 'AGS-CN-903', 'Wetsil Plus (Ethoxylated Trisilioxane Surfactant) 50 ml', 40),
      ('CG', 'Agrostar WSF', 'AGS-CN-820', 'NPK 19:19:19 (TE)1 kg', 25),
      ('CG', 'Agmix', 'AGS-CP-1472', 'Agmix (Metsulfuron Methyl 10% + Chlorimuron Ethyl 10% WP) 8 gm', 25),

      ('CG', 'Amaze-X', 'AGS-CP-1261', 'Amaze X (Emamectin benzoate 5% SG) 10 g', 150),
      ('CG', 'Consta', 'AGS-CP-1744', 'Consta (Fipronil 40% + Imidacloprid 40% WG) 10 g', 150),
      ('CG', 'Cruzer', 'AGS-CP-955', 'Cruzer (Thiamethoxam 25% WG) 5 g', 300),
      ('CG', 'Glyclean', 'AGS-CP-1218', 'Glyclean (Glyphosate 41 %) 5 litre', 2),
      ('CG', 'Glyclean 71', 'AGS-CP-1263', 'Glyclean 71 (Glyphosate 71% SG) 500 g', 20),

      ('CG', 'Glyclean Power', 'AGS-CP-1555', 'Glyclean Power (Glufosinate Ammonium 13.5 SL) 1 litre', 10),
      ('CG', 'Kleenweed', 'AGS-CP-1145', 'Kleenweed (2-4-D Amine Salt 58% SL) 400 ml', 10),
      ('CG', 'Mandoz', 'AGS-CP-949', 'Mandoz (Mancozeb 63% + Carbendazim 12% WP) 1 kg', 6),
      ('CG', 'MetalGRO', 'AGS-CP-901', 'MetalGRO (Metalaxyl 8% + Mancozeb 64% WP) 250 g', 40),
      ('CG', 'Paraquit', 'AGS-CP-1310', 'Paraquit (Paraquat Dichloride 24% SL) 1 litre', 6),

--10. HR
      ('HR', 'Agrostar WSF', 'AGS-CN-978', 'Calcium Nitrate 10 kg', 1),
      ('HR', 'Power Gel', 'AGS-CN-801', 'Power Gel (Organic Plant Nutrient) 40 g Sachet', 25),
      ('HR', 'Nutripro Zinc HEDP', 'AGS-CN-876', 'Nutripro Zinc ( Zn 17% HEDP ) 250 g', 16),
      ('HR', 'Bhumika', 'AGS-CN-860', 'Bhumika (Organic Plant Vitaliser) 8N X 4kg Drum 32 kg', 1),
      ('HR', 'NutriPro Magnesium Sulphate', 'AGS-CN-765', 'Nutri Pro Magnesium Sulphate 1 kg', 20),
     
      ('HR', 'Sanchaar', 'AGS-CN-954', 'Sanchaar (Bio-Enriched Organic Manure) 5 kg', 10),
      ('HR', 'Florence', 'AGS-CN-777', 'Florence (Biostimulants) 500 ml', 10),
      ('HR', 'Kataar', 'AGS-CN-972', 'Kataar (Paclobutrazole 23% SC) 1 litre', 6),
      ('HR', 'Nutripro Iron HEDP', 'AGS-CN-873', 'Nutripro Iron ( Fe 17% HEDP ) 250 g', 16),
      ('HR', 'Nanovita Calcium', 'AGS-CN-772', 'Nanovita CA 11(Gluconolactate Liquid Calcium 11%) 500 ml', 8),
     
      ('HR', 'Nanovita Boron', 'AGS-CN-815', 'Nanovita B10 (Boron Ethanolamine 10%) 250ml', 10),
      ('HR', 'Bhumika WSP', 'AGS-CN-951', 'Bhumika WSP 400 g', 20),
      ('HR', 'NPK HD', 'AGS-CN-786', 'NPK HD (15:30:05:01)250 gm', 4),
      ('HR', 'Humic Power NX', 'AGS-CN-713', 'Humic Power NX (Humic & Fulvic Derivative) 200g', 20),
      ('HR', 'Pure Kelp', 'AGS-CN-814', 'Pure Kelp (Seaweed Extract) 250 ml', 16),
     
      ('HR', 'Karbonik', 'AGS-CN-774', 'Karbonik C:N (Organic Carbon 14%) 1 litre', 10),
      ('HR', 'Peak Booster', 'AGS-CN-791', 'Peak Booster (Triacontanol 0.1% EW) 500 ml', 8),
      ('HR', 'Selzic', 'AGS-CN-643', 'Selzic (Sulphur 65% + Zinc 18% Granules) 3 kg', 4),
      ('HR', 'Faster', 'AGS-CN-738', 'Faster (Biostimulant) 250ml', 16),
      ('HR', 'Hold On', 'AGS-CN-794', 'Hold On (Alpha Naphthyl Acetic Acid 4.5% SL) 100 ml', 40),

--11. BH
      ('BH', 'Agronil Gr', 'AGS-CP-1264', 'Agronil GR (Fipronil 0.3% GR) 1 kg', 25),
      ('BH', 'Adonix Neo', 'AGS-CP-1634', 'Adonix Neo (Pyriproxyfen 10% + Bifenthrin 10% EC) 100 ml', 100),
      ('BH', 'Consta', 'AGS-CP-605', 'Consta (Fipronil 40% + Imidacloprid 40% WG) 40 g', 20),
      ('BH', 'Agrostar WSF', 'AGS-CN-820', 'NPK 19:19:19 (TE)1 kg', 25),
      ('BH', 'Stelar', 'AGS-CN-308', 'Stelar (Giberellic Acid 0.001% L) 500 ml', 8),
     
      ('BH', 'Cruzer', 'AGS-CP-955', 'Cruzer (Thiamethoxam 25% WG) 5 g', 300),
      ('BH', 'Adonix', 'AGS-CP-958', 'Adonix (Pyriproxyfen 5% + Diafenthiuron 25% SE) 100 ml', 20),
      ('BH', 'Bhumika', 'AGS-CN-860', 'Bhumika (Organic Plant Vitaliser) 8N X 4kg Drum 32 kg', 1),
      ('BH', 'Capasity', 'AGS-CP-1506', 'Capasity (Captan 70% + Hexaconazole 5% WP) 250 g', 16),
      ('BH', 'Humic Power NX', 'AGS-CN-713', 'Humic Power NX (Humic & Fulvic Derivative) 200g', 20),
     
      ('BH', 'Pure Kelp', 'AGS-CN-814', 'Pure Kelp (Seaweed Extract) 250 ml', 16),
      ('BH', 'Wetsil Plus', 'AGS-CN-656', 'Wetsil Plus (Ethoxylated Trisilioxane Surfactant)250 ml', 8),
      ('BH', 'Power Gel', 'AGS-CN-869', 'Power Gel 40gX50 N(2kg) Saathi Pack (MRP Rs.75/N)', 1),
      ('BH', 'Dragnet', 'AGS-CP-1392', 'Dragnet (Azoxystrobin 4.8% + Chlorothalonil 40 % SC) 150 ml', 15),
      ('BH', 'Agronil 80', 'AGS-CP-1456', 'Agronil 80 (Fipronil 80 WG) 2 g', 100),
     
      ('BH', 'Arex 505', 'AGS-CP-938', 'Arex 505 (Chlorpyriphos 50% + Cypermethrin 5% EC) 250 ml', 16),
      ('BH', 'Amaze-X', 'AGS-CP-1261', 'Amaze X (Emamectin benzoate 5% SG) 10 g', 150),
      ('BH', 'Dyna Shield', 'AGS-CP-1603', 'Dyna Shield (Dinotefuran 20% SG) 10 g', 100),
      ('BH', 'Faster', 'AGS-CN-858', 'Faster (Biostimulant) 1 litre', 4),
      ('BH', 'Danzhi', 'AGS-CP-1540', 'Danzhi (Clothianidin 50% WG) 6 g', 50)



])
)
 b
  ON
      a.State = b.State
))a
left join (SELECT
  uuid,
  cartCriteria_constraints_constraintList_params_itemQuanList_skuCode,right(source,2) as state,
  source
FROM (
  SELECT
    uuid,
    cartCriteria_constraints_constraintList_params_itemQuanList_skuCode,
    SPLIT(REPLACE(REPLACE(sources, '[', ''), ']', ''), ',') AS source_array
  FROM
    agrostar-data.prod_db_views.promotion
  WHERE
    LOWER(sources) LIKE "%b2b%"
    AND status IN ("ACTIVATED")
    -- and date(createdon)='2024-12-12'
    AND DATE(validTo) > CURRENT_DATE() - 1
    AND type = "OFFER"
)
CROSS JOIN UNNEST(source_array) AS source)b
on a.state=b.state and a.Sku_code=b.cartCriteria_constraints_constraintList_params_itemQuanList_skuCode
order  by 1
)
) as fixpg

 on ord.reference_customer_id=fixpg.farmerid
 )) 
where
  farmerId is not null
order by 1
