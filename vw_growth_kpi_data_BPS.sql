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
, CASE WHEN parent.DoubleCount_Fix=1 THEN parent.paid_amount                                  ELSE 0  END AS paid_amount

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
                    (SELECT
                      data_prep.*
                      , CASE WHEN psm.PartnerShipSuccessManager IS NULL
                            THEN 'unallocated'
                        ELSE psm.PartnerShipSuccessManager
                   END                                                                                             AS PartnerShipSuccessManager


	            FROM
              data_prep
              LEFT JOIN
              `gc-data-infrastructure-7e07.experimental_tables.App_Name_PSM_Lookup`  psm
               ON
               data_prep.signup_app_name = psm.app_name
)

SELECT
DISTINCT
  psm.*
, wd.is_working_day
, wd.working_day_of_month
FROM
psm
LEFT JOIN

`gc-data-infrastructure-7e07.import.working_days` wd
ON psm.kpi_day = date(wd.calendar_date)
AND
CASE WHEN psm.scheme IS NULL THEN 'bacs' WHEN psm.scheme ='sepa_core' THEN 'sepa' ELSE psm.scheme end = wd.scheme
;
