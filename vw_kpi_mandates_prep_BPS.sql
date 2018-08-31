-- vw_kpi_mandates_prep.sql

SELECT DATE(mandates.created_at) kpi_day
       , mandates.organisation_id
       , scheme
       , COUNT(distinct mandates.id) mandates_created

    FROM `gc-data-infrastructure-7e07.materialized_views.vw_mandates` mandates
         LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_organisation_states` organisation_states
                   ON organisation_states.organisation_id = mandates.organisation_id
                      AND mandates.created_at BETWEEN organisation_states.state_start_at
                                                      AND COALESCE(organisation_states.state_end_at, current_timestamp())
   WHERE organisation_states.to_state = 'active'
GROUP BY DATE(mandates.created_at)
       , mandates.organisation_id
       , scheme
;
