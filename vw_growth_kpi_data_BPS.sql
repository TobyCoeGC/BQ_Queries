-- vw_growth_kpi_data_by_partner.sql

--- reflatten fixed_scheme_mandates_created
 -- tested this against flattened aggregate and against the acid test of mandateSchemeFix "Pairing" recompressing the original rows with their fixed 'scheme/mandates_created' values
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
                   END                                      AS PartnerShipSuccessManager
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


, ActiveDivider AS                        (                        
                                            SELECT
                                              CAST(COUNT(*) AS FLOAT64)        AS ActiveDivider
                                            , CAST(MAX(IFNULL(Active,0))  AS FLOAT64)    AS ActiveConstant
                                            , KPI_DAY
                                            , organisation_id
                                            FROM
                                            psm
                                            WHERE 
                                            organisation_id IS NOT NULL
                                            GROUP BY
                                             KPI_DAY
                                           , organisation_id 
                                         )
                                             

, Final AS
               (
                 SELECT
                   pre_divide.* EXCEPT (Active)
                 , IEEE_DIVIDE(ActiveDivider.ActiveConstant,ActiveDivider.ActiveDivider ) as Active
                 FROM
                 psm pre_divide
                 JOIN
                 ActiveDivider
                 USING
                 (organisation_id, kpi_day)
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
