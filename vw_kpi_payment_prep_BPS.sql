-- vw_kpi_payment_prep.sql

SELECT
         kpi_day
       , organisation_id
       , to_state

       , partner_id
       , partner_name
       , scheme
       , Partner_fees

       , SUM(paid)            AS paid
       , SUM(submitted)       AS submitted
       , SUM(chargeback)      AS chargeback
       , SUM(late_failure)    AS late_failure
       , SUM(failed)          AS failed
       , SUM(rejected)        AS rejected
       , SUM(paid_amount)     AS Paid_amount
       FROM

          (
          SELECT DATE(created_at) kpi_day
                 , pmt_actions.organisation_id

                 -- > V2
                 , pmt_actions.partner_id
                 , pmt_actions.scheme
                 , SUM(pmt_Actions.partner_fees) AS Partner_fees
                 , pmt_actions.partner_name
                 , SUM(pmt_Actions.paid_amount) AS Paid_amount
                 -- / V2

                 , daily_states.to_state
                 -- distinct count because some payments have > 1 of the same payment action on the same day
                 , COALESCE(COUNT(DISTINCT(CASE WHEN payment_actions_to_state = 'submitted' THEN payment_id END)), 0) AS submitted
                 , COALESCE(COUNT(DISTINCT(CASE WHEN payment_actions_to_state = 'paid' THEN payment_id END)), 0) AS paid
                 , COALESCE(COUNT(DISTINCT(CASE WHEN payment_actions_to_state = 'charged_back' THEN payment_id END)), 0) AS chargeback
                 , COALESCE(COUNT(DISTINCT(CASE WHEN payment_actions_to_state = 'late_failure' THEN payment_id END)), 0) AS late_failure
                 , COALESCE(COUNT(DISTINCT(CASE WHEN payment_actions_to_state = 'failed' THEN payment_id END)), 0) AS failed
                 , COALESCE(COUNT(DISTINCT(CASE WHEN payment_actions_to_state = 'rejected' THEN payment_id END)), 0) AS rejected
              FROM `gc-data-infrastructure-7e07.experimental_tables.vw_payment_actions_with_states` pmt_actions
          -- We join onto the daily states view to ensure we use the last state a merchant was in for each kpi day
          -- This ensures we don't have duplicated records for one merchant on one day
          LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_kpi_daily_states` daily_states
                    ON daily_states.organisation_id = pmt_actions.organisation_id
                   AND daily_states.kpi_day = DATE(pmt_actions.created_at)
             WHERE payment_actions_to_state IN ('submitted', 'paid', 'late_failure', 'failed', 'rejected','charged_back')

          GROUP BY kpi_day
                 , organisation_id
                 , to_state
                 , partner_id
                 , partner_name
                 , scheme

          )

GROUP BY kpi_day
       , organisation_id
       , to_state
       , partner_id
       , partner_name
       , scheme
       , Partner_fees
;
