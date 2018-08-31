-- vw_kpi_revenue_prep.sql

WITH transaction_fees_cte AS (

      SELECT date(created_at) kpi_day
           , organisation_id
           , partner_id
           , scheme
           , SUM(transaction_fee * exchange_rates.rate) AS transaction_fee
        FROM `gc-data-infrastructure-7e07.experimental_tables.vw_payment_actions_with_states` payment_actions
             LEFT JOIN `gc-data-infrastructure-7e07.import.exchange_rates` exchange_rates
                       ON exchange_rates.calendar_date = timestamp_trunc(created_at, month)
                          AND exchange_rates.currency_from = payment_actions.transaction_fee_currency
                          AND exchange_rates.currency_to = 'GBP'
       WHERE transaction_fee <> 0
    GROUP BY kpi_day
           , organisation_id
           , partner_id
           , scheme

), monthly_fees_cte AS (

      SELECT date(created_at) kpi_day
           , organisation_id
            , partner_id
            , scheme
           , SUM(monthly_fee * exchange_rates.rate) AS monthly_fee
        FROM `gc-data-infrastructure-7e07.experimental_tables.vw_payment_actions_with_states` payment_actions
             LEFT JOIN `gc-data-infrastructure-7e07.import.exchange_rates` exchange_rates
                       ON exchange_rates.calendar_date = timestamp_trunc(created_at, month)
                          AND exchange_rates.currency_from = payment_actions.monthly_fee_currency
                          AND exchange_rates.currency_to = 'GBP'
       WHERE monthly_fee <> 0
    GROUP BY kpi_day
           , organisation_id
            , partner_id
            , scheme

), revenue_shares_cte AS (

      SELECT date(created_at) kpi_day
           , organisation_id
           , partner_id
           , scheme
           , SUM(revenue_share * exchange_rates.rate) AS revenue_share
        FROM `gc-data-infrastructure-7e07.experimental_tables.vw_payment_actions_with_states` payment_actions
             LEFT JOIN `gc-data-infrastructure-7e07.import.exchange_rates` exchange_rates
                       ON exchange_rates.calendar_date = timestamp_trunc(created_at, month)
                          AND exchange_rates.currency_from = payment_actions.revenue_share_currency
                          AND exchange_rates.currency_to = 'GBP'
       WHERE revenue_share <> 0
    GROUP BY kpi_day
           , organisation_id
           , partner_id
           , scheme

), app_fees_cte AS (

      SELECT date(created_at) kpi_day
           , organisation_id
           , partner_id
           , scheme
           , SUM(app_fee * exchange_rates.rate) AS app_fee
        FROM `gc-data-infrastructure-7e07.experimental_tables.vw_payment_actions_with_states` payment_actions
             LEFT JOIN `gc-data-infrastructure-7e07.import.exchange_rates` exchange_rates
                       ON exchange_rates.calendar_date = timestamp_trunc(created_at, month)
                          AND exchange_rates.currency_from = payment_actions.app_fee_currency
                          AND exchange_rates.currency_to = 'GBP'
       WHERE app_fee <> 0
    GROUP BY kpi_day
           , organisation_id
           , partner_id
           , scheme

)

SELECT organisation_id
     , kpi_day
     , partner_id
     , scheme
     , daily_states.to_state
     , COALESCE(transaction_fee, 0) AS transaction_fee
     , COALESCE(monthly_fee, 0) AS monthly_fee
     , COALESCE(revenue_share, 0) AS revenue_share
     , COALESCE(app_fee, 0) AS app_fee
  FROM transaction_fees_cte
       FULL OUTER JOIN monthly_fees_cte USING (organisation_id, kpi_day, partner_id, scheme)
       FULL OUTER JOIN revenue_shares_cte USING (organisation_id, kpi_day, partner_id , scheme)
       FULL OUTER JOIN app_fees_cte USING (organisation_id, kpi_day, partner_id , scheme)
       LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_kpi_daily_states` daily_states
                 USING (organisation_id, kpi_day)

;
