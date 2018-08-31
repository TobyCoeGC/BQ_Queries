-- vw_payment_actions_with_states.sql

WITH payment_actions_with_states AS (

    SELECT payment_actions_and_monthly_fees.* EXCEPT (to_state)
         , payment_actions_and_monthly_fees.to_state AS payment_actions_to_state
         , organisation_states.to_state
         , organisation_states.state_start_at
      FROM `gc-data-infrastructure-7e07.experimental_tables.vw_payment_actions_and_monthly_fees` payment_actions_and_monthly_fees
           LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_organisation_states` organisation_states
                      ON organisation_states.organisation_id = payment_actions_and_monthly_fees.organisation_id
                         AND payment_actions_and_monthly_fees.created_at BETWEEN organisation_states.state_start_at
                                                                AND COALESCE(organisation_states.state_end_at, current_timestamp())

), current_state_prep AS (

     -- can't calculate this in the first CTE as the current state may
     -- have taken place after the latest payment action
     SELECT organisation_id
          , to_state
          , RANK() OVER (PARTITION BY organisation_id ORDER BY organisation_states.state_start_at DESC) AS current_state_rank
       FROM `gc-data-infrastructure-7e07.materialized_views.vw_organisation_states` organisation_states

)

SELECT payment_actions_with_states.*
     , organisations.created_at AS signed_up_at
     , organisations.* EXCEPT (organisation_id, creditor_id, created_at)
     , current_state_prep.to_state AS current_state
  FROM payment_actions_with_states
       LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_organisations` organisations
                 USING (organisation_id)
       LEFT JOIN current_state_prep
                 ON current_state_prep.organisation_id = payment_actions_with_states.organisation_id
                 AND current_state_rank = 1
;
