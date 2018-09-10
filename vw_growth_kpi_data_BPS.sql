-- vw_growth_kpi_data_by_partner.sql

WITH Final AS
               (
                 SELECT
                   pre_divide.* EXCEPT (Active)
                 , IEEE_DIVIDE(ActiveDivider.ActiveConstant,ActiveDivider.ActiveDivider ) as Active
                 FROM
                 `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}..vw_growth_kpi_data_bps_prediv` pre_divide
              ) 



SELECT
DISTINCT
  final.* EXCEPT (ActiveConstant,ActiveDivider)
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
