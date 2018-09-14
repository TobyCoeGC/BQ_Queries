-- vw_payment_actions_and_monthly_fees.sql

WITH payment_actions_sorted_key AS (

                                    SELECT
                                    * EXCEPT(sort_key, refund_sort_key)
                                         -- Combine refunds.sort_key with payment_actions.sort_key
                                         -- We need to make sure the resulting sort key is unique
                                         -- But also are correct chronologically
                                         -- so we try to find the last sort_key before the refund, and then add +1 to the key
                                         -- we use (refund_sort_key / 10) so 2 partial refunds don't get the same sort_key
                                         , CASE WHEN to_state IN ('refund', 'partial_refund')
                                                THEN CAST(MAX(sort_key) OVER (PARTITION BY payment_id ORDER BY created_at)
                                                          + (refund_sort_key / 10) + 1 AS INT64)
                                                ELSE sort_key
                                           END AS sort_key
                                      FROM `gc-data-infrastructure-7e07.experimental_tables.vw_payment_actions_and_monthly_fees_prep`
                                      WHERE source_table='payment_actions'
                                   )


, payment_actions_sorted  AS  (
                                SELECT * EXCEPT( sort_key, refund_sort_key)
                                      , sort_key
                                      FROM `gc-data-infrastructure-7e07.experimental_tables.vw_payment_actions_and_monthly_fees_prep`
                                      WHERE source_table!='payment_actions'
                                      
                                 UNION ALL
                                      
                                 SELECT * FROM payment_actions_sorted_key
                              )

 , payment_actions_with_submission_count AS (

    SELECT payment_actions_sorted.*
         -- each time a payment is retried, we can see this in the cause
         , COUNTIF(cause='payment_retried') OVER (PARTITION BY payment_id ORDER BY sort_key) + 1 AS submission_count
      FROM payment_actions_sorted

), payment_submissions_with_previous AS (

    SELECT *
         , LAG(created_at) OVER (PARTITION BY payment_id, submission_count ORDER BY sort_key) previous_at
      FROM payment_actions_with_submission_count

), payment_submissions_day_calculations AS (

    SELECT *
         , TIMESTAMP_DIFF(created_at, previous_at, DAY) days_to_previous_state
           -- TODO: make this working day calculation much more efficient
         , CASE WHEN previous_at IS NULL THEN NULL
                WHEN TIMESTAMP_TRUNC(previous_at, DAY) = TIMESTAMP_TRUNC(created_at, DAY)
                     THEN 0
                ELSE (SELECT SUM(CASE WHEN is_working_day THEN 1 ELSE 0 END)
                        FROM `gc-data-infrastructure-7e07.import.working_days` working_days
                       WHERE working_days.calendar_date BETWEEN payment_submissions_with_previous.previous_at
                                                                AND payment_submissions_with_previous.created_at
                             AND working_days.scheme = payment_submissions_with_previous.scheme
                     ) END AS working_days_to_previous_state
      FROM payment_submissions_with_previous

)

SELECT *
     , SUM(working_days_to_previous_state) OVER (PARTITION BY
                payment_id, submission_count ORDER BY sort_key) working_days_running_total
     , SUM(days_to_previous_state) OVER (PARTITION BY
                payment_id, submission_count ORDER BY sort_key) days_running_total
  FROM payment_submissions_day_calculations
;
