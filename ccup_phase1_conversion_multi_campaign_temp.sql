-- Under the Final section of this query, filter out on campaign names and category, under the WHERE clause comment in the rows not utilized

create or replace table `wmt-wmg-adops-adhoc-dev.mi_adhoc.post_hbo_pebbles_knb_conversion_temp` as 

# 1 attribution
WITH attribution AS (
SELECT
  camp.campaign_name,
  camp.external_campaign_id,
  attri.luid,
  attri.sales_channel,
  attri.itemset_priority,
  SUM(attri.attributed_amount) AS attributed_amount
FROM
  `wmt-84fe52fae01cc3d4b5e52e8625.wmg_wmx.wmx_subcampaigns` AS subcamp
INNER JOIN (
  SELECT
    external_subcampaign_id,
    MAX(version) AS max_version
  FROM
    `wmt-84fe52fae01cc3d4b5e52e8625.wmg_wmx.wmx_subcampaigns`
  GROUP BY
    1 ) AS version_control
ON
  subcamp.external_subcampaign_id = version_control.external_subcampaign_id
  AND subcamp.version = version_control.max_version
INNER JOIN
  `wmt-84fe52fae01cc3d4b5e52e8625.wmg_wmx.campaigns_latest` AS camp
ON
  subcamp.external_campaign_id = camp.external_campaign_id
  # camp.external_campaign_id IN ("34108","fxvg929")
  # AND camp.campaign_name = "Henkel Laundry WSL 1Q Jan 2023_42515"
INNER JOIN
  `wmt-84fe52fae01cc3d4b5e52e8625.wmg_wmx.attributed_transactions_latest_v2` attri
ON
  subcamp.subcampaign_number = CAST(attri.internal_subcampaign_id AS INT64)
WHERE 1=1
  AND attri.attribution_strategy = 'fair14partner'
  AND attri.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND CURRENT_DATE()
GROUP BY
  1,2,3,4,5
  ),
   
# 2 campaign category
 category AS (
   SELECT
        aa.camp_name AS campaign_name,
        bb.opp_adv_name AS advertiser_name,
        aa.camp_start_dt as start_date,
        aa.camp_end_dt as end_date,
        bb.opp_adv_cat_lvl_0 AS division_L0,
        bb.opp_adv_cat_lvl_1 AS department_L1,
        bb.opp_adv_cat_lvl_2 AS category_L2,
        bb.opp_adv_cat_lvl_3 AS subcategory_L3
      FROM
        `wmt-84fe52fae01cc3d4b5e52e8625.wmg_edw_prod.d_campaign` AS aa
      LEFT JOIN
        `wmt-84fe52fae01cc3d4b5e52e8625.wmg_edw_prod.d_opportunity` AS bb
        ON aa.camp_pio_opportunity_id = bb.opp_pio_id
      WHERE
        aa.camp_pio_opportunity_id IS NOT NULL
        AND aa.camp_pio_opportunity_id > 1
        AND aa.src_rec_present_in_prev_refresh = "yes"
        AND bb.src_rec_present_in_prev_refresh = "yes"
 ),

#3 experian table processing - collapse the indiv id level to LUID level (1 luid per row)
luid_attributes AS (
with gender_count AS (
SELECT
    LVNG_UNIT_ID 
   , SUM(CASE WHEN GENDER_DESC = 'FEMALE' THEN 1 ELSE 0 END) as F_Count
   , SUM(CASE WHEN GENDER_DESC = 'MALE' THEN 1 ELSE 0 END) as M_Count
FROM
    `wmt-edw-prod.US_EXPERIAN_OV_DL_SECURE.CUST_EXPERIAN_PROFL`
GROUP BY 1
),
ranking AS (
SELECT
  LVNG_UNIT_ID
  , GENDER_DESC
  -- , HH_EST_INCOME_CD
  , edu_lvl_desc 
  , ST_PROV_CD 
  , HH_EST_INCOME_DESC
  , EXCT_AGE_QTY
  , MRTL_STATUS_DESC
  , SAFE_CAST(HH_ADLT_QTY AS INT64) + SAFE_CAST(HH_CHLD_QTY AS INT64) as household_size  
  , SAFE_CAST(HH_CHLD_QTY AS INT64) as number_children
  , CASE
    WHEN F_Count > M_Count
      THEN ROW_NUMBER ()
          Over (PARTITION BY  LVNG_UNIT_ID 
                ORDER BY CASE WHEN a.gender_desc = 'FEMALE' THEN a.EXCT_AGE_QTY END DESC)
    WHEN F_Count < M_Count
  THEN ROW_NUMBER ()
          Over (PARTITION BY  LVNG_UNIT_ID 
                ORDER BY CASE WHEN a.gender_desc = 'MALE' THEN a.EXCT_AGE_QTY END DESC)
    WHEN F_Count = M_Count
    THEN ROW_NUMBER ()
          Over (PARTITION BY  LVNG_UNIT_ID 
                ORDER BY a.EXCT_AGE_QTY DESC)
      ELSE 0 END as ranked
FROM  `wmt-edw-prod.US_EXPERIAN_OV_DL_SECURE.CUST_EXPERIAN_PROFL` a
      left join gender_count b using (LVNG_UNIT_ID )
)
SELECT
  cast( LVNG_UNIT_ID as string) as luid
  , ranked
  , GENDER_DESC as gender
  , CASE
      WHEN HH_EST_INCOME_DESC = 'UNKOWN' THEN "Unknown"
      WHEN HH_EST_INCOME_DESC = '1000-14999'THEN "Low"
      WHEN HH_EST_INCOME_DESC = '15000-24999' THEN "Low"
      WHEN HH_EST_INCOME_DESC = '25000-34999'THEN "Low"
      WHEN HH_EST_INCOME_DESC = '35000-49999'THEN "Low"
      WHEN HH_EST_INCOME_DESC = '50000-74999'THEN "Medium"
      WHEN HH_EST_INCOME_DESC = '75000-99999'THEN "Medium"
      WHEN HH_EST_INCOME_DESC = '100000-124999'THEN "High" 
      WHEN HH_EST_INCOME_DESC = '125000-149999'THEN "High"
      WHEN HH_EST_INCOME_DESC = '150000-174999'THEN "High"
      WHEN HH_EST_INCOME_DESC = '175000-199999'THEN "High"
      WHEN HH_EST_INCOME_DESC = '200000-249999' THEN "High"
      WHEN HH_EST_INCOME_DESC = '250000+'THEN "High"
      ELSE "Null"
      END AS income
  , edu_lvl_desc as education
  , ST_PROV_CD as state_code
  , CASE
      WHEN safe_cast(EXCT_AGE_QTY AS INT64) < 18 THEN "Under 18"
      WHEN safe_cast(EXCT_AGE_QTY AS INT64) >= 18 AND safe_cast(EXCT_AGE_QTY AS INT64) <= 25 THEN "18- to 25-year-olds"
      WHEN safe_cast(EXCT_AGE_QTY AS INT64) >= 26 AND safe_cast(EXCT_AGE_QTY AS INT64) <= 40 THEN "26- to 40-year-olds"
      WHEN safe_cast(EXCT_AGE_QTY AS INT64) >= 41 AND safe_cast(EXCT_AGE_QTY AS INT64) <= 60 THEN "41- to 60-year-olds"
      WHEN safe_cast(EXCT_AGE_QTY AS INT64) >= 61 THEN "61-year-olds and Up"
      ELSE "NULL"
      END AS age
  ,  MRTL_STATUS_DESC as marital_status
  ,  household_size
  ,  number_children
FROM ranking
where ranked = 1 -- ensure 1 record(with higher gender count and top age)  is selected to represent its Luid
) 
# FINAL  join 1,2,3 above: aggregate campaign on luid attributes, shrinks the table size
SELECT
  b.ranked
  , 'advertiser' as advertiser_name
  , 'campaign' as campaign_name
  , CURRENT_DATE() as start_date
  , CURRENT_DATE() as end_date
  , 'division' AS division_L0
  , 'department' AS department_L1
  , 'category' AS category_L2
  , 'subcategory' AS subcategory_L3
  # new dimensions
  , 'sales channel' as sales_channel
  , 'itemset_priority' as itemset_priority
  , b.* EXCEPT(luid, ranked)
  , current_date() as insert_date
  , SUM(a.attributed_amount) as attributed_sales
  , COUNT(DISTINCT a.luid) as unique_hhs
FROM
   attribution a
   left join luid_attributes b using(luid)
   left join category c using(campaign_name)
  WHERE a.external_campaign_id IN ("Uzcx7sa", "7k1dr52", "Sn53de9")
-- WHERE a.campaign_name IN ("Beiersdorf_Eucerin_HBL_Q4_FY25_56202","Beiersdorf_Coppertone Face Display_Q2-Q3_FY25_51216")
-- AND  c.subcategory_L3 IN ("HAND AND BODY LOTION")
GROUP BY
  1,2,3, 4,5,6,7,8,9, 10,11, 12,13,14,15,16,17,18,19, 20
;
