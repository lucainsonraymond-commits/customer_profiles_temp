-- Under the CTE exposed luids filter out on campaign names and category, comment out the filter row not utilized
create or replace table `wmt-wmg-adops-adhoc-dev.gcp_wmg_dev_mmt.post_hbo_pebbles_knb_exposure_temp` as
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
),
luid_attributes AS (
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
),
advertiser_hierarchy AS(
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
      GROUP BY 1,2,3,4,5,6,7,8
),
exposed_luids AS (
SELECT
luid, camp_id
FROM `wmt-84fe52fae01cc3d4b5e52e8625.wmg_edw_prod.disp_cust_impressions_summary`
WHERE (event_dt BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND CURRENT_DATE())
 AND luid <> 'N/A'
 AND camp_id IN ("Uzcx7sa", "7k1dr52", "Sn53de9")
 AND categ_nm IN ("COLD CEREAL L3", "DRY DOG FOOD")
GROUP BY 1,2
UNION DISTINCT
SELECT
luid, camp_id
FROM `wmt-84fe52fae01cc3d4b5e52e8625.wmg_edw_prod.dsp_cust_impressions_summary`
WHERE (event_dt BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND CURRENT_DATE())
 AND luid <> 'N/A'
 AND camp_id IN ("Uzcx7sa", "7k1dr52", "Sn53de9")
 AND categ_nm IN ("COLD CEREAL L3", "DRY DOG FOOD")
GROUP BY 1,2
)
SELECT 'campaign' as campaign_name,'advertiser' as advertiser_name,CURRENT_DATE() as start_date,CURRENT_DATE() as end_date,'division' as division_L0,'department'as department_L1,'category' as category_L2,'subcategory' as subcategory_L3, a.* EXCEPT(luid), current_date() as insert_date, COUNT(DISTINCT a.luid) as unique_hhs
FROM luid_attributes a
INNER JOIN exposed_luids b
ON a.luid = b.luid
--LEFT JOIN advertiser_hierarchy c
--ON b.camp_nm = c.campaign_name
GROUP BY 1,2,3, 4,5,6,7,8,9, 10,11,12,13,14,15,16,17,18
;
