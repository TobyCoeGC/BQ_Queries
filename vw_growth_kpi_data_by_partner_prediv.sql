WITH data_prep AS

                (
                SELECT
                  DISTINCT
                  parent.* EXCEPT (mandates_created, scheme, mandateSchemeFix, active, inactive, activated, monthly_fee_active)
                , fix.mandates_created
                , fix.scheme
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.active                                       ELSE 0  END AS active
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.inactive                                     ELSE 0  END AS inactive
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.activated                                    ELSE 0  END AS activated
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.monthly_fee_active                           ELSE 0  END AS monthly_fee_active


                FROM
                `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_growth_kpi_data_by_partner_prep` fix

                JOIN

                `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_growth_kpi_data_by_partner_prep` parent

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
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.active                                       ELSE 0  END AS active
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.inactive                                     ELSE 0  END AS inactive
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.activated                                    ELSE 0  END AS activated
                , CASE WHEN parent.DoubleCount_Fix=1 THEN parent.monthly_fee_active                           ELSE 0  END AS monthly_fee_active


                FROM

                `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_growth_kpi_data_by_partner_prep` parent
                LEFT JOIN
                (
                    SELECT
                      kpi_day
                    , organisation_id
                    FROM

                    `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_growth_kpi_data_by_partner_prep`
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

                , CASE WHEN psm.PSM IS NULL
                            THEN 'unallocated'
                        ELSE psm.PSM
                   END                                                                      AS PartnerShipSuccessManager
	              FROM
                data_prep
                LEFT JOIN
                ( SELECT DISTINCT
                    PSM
                   ,parent_app_name
                  FROM
                 `gc-data-infrastructure-7e07.experimental_tables.segmentation_mapping_partners`
                 )  psm
                ON
                data_prep.signup_app_name = psm.parent_app_name
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
                                                  `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_growth_kpi_data_by_partner_prep`
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
                                                  `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_growth_kpi_data_by_partner_prep`
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

, pre_final   AS                          (
                                              SELECT
                                                db.* EXCEPT (active, reactivated, deactivated, activated)
                                              , CASE
                                                    WHEN db.doublecount_null_fix = 1
                                                          THEN 0
                                                    ELSE db.active
                                                END                                     AS active
                                                , CASE
                                                    WHEN db.doublecount_null_fix = 1
                                                          THEN 0
                                                    ELSE db.reactivated
                                                END                                     AS reactivated

                                                , CASE
                                                    WHEN db.doublecount_null_fix = 1
                                                          THEN 0
                                                    ELSE db.deactivated
                                                END                                     AS deactivated

                                                , CASE
                                                    WHEN db.doublecount_null_fix = 1
                                                          THEN 0
                                                    ELSE db.activated
                                                END                                     AS activated


                                              FROM
                                              doublecount_null_partner_dual_scheme db
                                           )


, ActiveDivider AS                        (                        
                                            SELECT
                                              CAST(COUNT(*) AS FLOAT64)        AS ActiveDivider
                                            , CAST(MAX(IFNULL(Active,0))  AS FLOAT64)    AS ActiveConstant
                                            , KPI_DAY
                                            , organisation_id
                                            FROM
                                            pre_final
                                            WHERE organisation_id IS NOT NULL
                                            GROUP BY
                                             KPI_DAY
                                           , organisation_id 
                                         )
                                             


  SELECT
     pre_final.* 
   , ActiveConstant
   , ActiveDivider
   FROM
   pre_final
   LEFT JOIN
   ActiveDivider
   USING ( KPI_DAY, organisation_id )
