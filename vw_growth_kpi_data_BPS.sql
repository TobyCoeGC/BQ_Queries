-- vw_growth_kpi_data_by_partner.sql

-- reflatten fixed_scheme_mandates_created
 -- tested this against flattened aggregate and against the acid test of mandateSchemeFix "Pairing" recompressing the original rows with their fixed 'scheme/mandates_created' values
WITH data_prep AS

                (
                SELECT 
                  DISTINCT 
                  parent.* EXCEPT (mandates_created, scheme, mandateSchemeFix, active, inactive, activated, monthly_fee_active)
                , fix.mandates_created
                , fix.scheme
                --, -1 AS mandateSchemeFix -- to prove that this has worked, only values of -1 and 0 will come out in the final results
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.active                                       ELSE 0  END AS active
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.inactive                                     ELSE 0  END AS inactive
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.activated                                    ELSE 0  END AS activated
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.monthly_fee_active                           ELSE 0  END AS monthly_fee_active


                FROM 
                `gc-data-infrastructure-7e07.experimental_tables.vw_growth_kpi_data_by_partner_prep` fix

                JOIN

                `gc-data-infrastructure-7e07.experimental_tables.vw_growth_kpi_data_by_partner_prep` parent

                ON 
                fix.kpi_day = parent.kpi_day
                AND
                fix.organisation_id = parent.organisation_id

                WHERE
                fix.MandateSchemeFix = 1
                AND
                parent.MandateSchemeFix = 0
                and 
                parent.scheme IS NULL
                and
                parent.mandates_created IS NULL

                UNION DISTINCT



                SELECT 
                  DISTINCT 
                  parent.* EXCEPT (mandates_created, scheme, mandateSchemeFix, active, inactive, activated, monthly_fee_active)
                , parent.mandates_created
                , parent.scheme
                --, parent.mandateSchemeFix
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.active                                       ELSE 0  END AS active
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.inactive                                     ELSE 0  END AS inactive
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.activated                                    ELSE 0  END AS activated
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.monthly_fee_active                           ELSE 0  END AS monthly_fee_active


                FROM 

                `gc-data-infrastructure-7e07.experimental_tables.vw_growth_kpi_data_by_partner_prep` parent
                LEFT JOIN
                (
                    SELECT
                      kpi_day
                    , organisation_id
                    FROM

                    `gc-data-infrastructure-7e07.experimental_tables.vw_growth_kpi_data_by_partner_prep`
                    WHERE
                    MandateSchemeFix = 1
                ) fix
                ON 
                fix.kpi_day = parent.kpi_day
                AND
                fix.organisation_id = parent.organisation_id

                WHERE
                parent.MandateSchemeFix = 0
                and 
                fix.kpi_day IS NULL
                and
                fix.organisation_id IS NULL
                )

, psm AS 
              (
                SELECT
                  data_prep.*                                                               
               
                , CASE WHEN psm.PartnerShipSuccessManager IS NULL 
                            THEN 'unallocated'
                        ELSE psm.PartnerShipSuccessManager 
                   END                                                                      AS PartnerShipSuccessManager
	              FROM 
                data_prep                           
                LEFT JOIN
                `gc-data-infrastructure-7e07.experimental_tables.App_Name_PSM_Lookup`  psm
                ON
                data_prep.signup_app_name = psm.app_name
              )

-- anomalous 2000 or so rows with double counting 'active' flag on an edgecase of null partner lines, caused by days where mandates are created on a scheme level, but there is no other activity on that day
, doublecount_null_partner_dual_scheme AS
                                          (
                                               SELECT 
                                                 psm.*
                                               , row_number () OVER (PARTITION BY organisation_id, kpi_day ORDER BY SCHEME) AS doublecount_null_fix
                                               FROM
                                               psm

                                               JOIN
                                              (
                                                  SELECT
                                                    organisation_id
                                                  , kpi_day
                                                  FROM
                                                  `gc-data-infrastructure-7e07.experimental_tables.vw_growth_kpi_data_by_partner`
                                                  WHERE
                                                  doublecount_fix = 1 
                                                  AND 
                                                  scheme IS NOT NULL
                                                  GROUP BY
                                                    organisation_id
                                                  , kpi_day

                                                  HAVING count(*) >1
                                              ) dupes

                                              USING (  organisation_id, kpi_day )
                                              
                                              UNION ALL
                                              
                                              SELECT 
                                                 psm.*
                                               , NULL AS doublecount_null_fix
                                               FROM
                                               psm

                                               LEFT JOIN
                                              (
                                                  SELECT
                                                    organisation_id
                                                  , kpi_day
                                                  FROM
                                                  `gc-data-infrastructure-7e07.experimental_tables.vw_growth_kpi_data_by_partner`
                                                  WHERE
                                                  doublecount_fix = 1 
                                                  AND 
                                                  scheme IS NOT NULL
                                                  GROUP BY
                                                    organisation_id
                                                  , kpi_day

                                                  HAVING count(*) >1
                                              ) dupes

                                              USING (  organisation_id, kpi_day )
                                              WHERE
                                              dupes.organisation_id IS NULL
                                              AND
                                              dupes.kpi_day         IS NULL  
                                          )

, final   AS                             (
                                          SELECT 
                                            db.* EXCEPT (active)
                                          , CASE 
                                                WHEN db.doublecount_null_fix = 1 
                                                      THEN 0
                                                ELSE db.active
                                            END                                     AS active

                                          FROM
                                          doublecount_null_partner_dual_scheme db

                                         )
              
SELECT
DISTINCT
  final.*
, wd.is_working_day
, wd.working_day_of_month
FROM
final
LEFT JOIN
`gc-data-infrastructure-7e07.import.working_days` wd
ON final.kpi_day = date(wd.calendar_date)
AND
CASE WHEN final.scheme IS NULL THEN 'bacs' WHEN final.scheme ='sepa_core' THEN 'sepa' ELSE final.scheme end = wd.scheme
;
