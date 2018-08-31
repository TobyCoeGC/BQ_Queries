-- vw_payment_actions_and_monthly_fees_prep.sql

WITH payment_filter AS (

    SELECT payments.*
         , mandates.scheme
      FROM `gc-data-infrastructure-7e07.gc_paysvc_live_production.payments` payments
           JOIN `gc-data-infrastructure-7e07.gc_paysvc_live_production.mandates` mandates
                ON mandates.id = payments.mandate_id

), payment_actions_prep AS (

    SELECT payment_actions.id
         , 'payment_actions' AS source_table
         , payment_id

         -- new code > 31-07-2018

         , CASE WHEN COALESCE(payments.app_id, creditor_partner_id) IN (   'AP00001079YASK'  # Bulk Submissions Tool
                                                                                , 'AP000010AAW117'  # Bulk changes
                                                                                , 'AP0000108FCZ26'  # Antilope
                                                                                , 'CR000028WQ50QW'  # Payyr
                                                                                , 'CR00001R5XEA5H'  # Billy / GC Dashboard

                                                                            --   , '0FP9H2NQBV'      # Payyr
                                                                            --   , '098SAA78BK'      # Billy / GC Dashboard
                                                                            )
                    THEN 'Internal - GC'

              WHEN salesforce_all.integration_name IN   (
                                                            'Zuora'
                                                          , 'Asperato'
                                                          , 'Junifer Systems'
                                                          , 'iRaiser'
                                                          , 'Vindicia'
                                                        )
                THEN CONCAT('placeholder_id_for_' , salesforce_all.integration_name)

              /* V4 testing 13-08-2018
              WHEN payments.source IN ('api' , 'dashboard')
               THEN 'Self-Serve'
              */

              ELSE COALESCE(payments.app_id, creditor_partner_id, 'unspecified')

         END AS partner_id
         -- For v1 apps, we're using the creditor's name, since we assume we can't
         -- access the v1 apps data.

       , CASE WHEN COALESCE(payments.app_id, creditor_partner_id) IN (          'AP00001079YASK'  # Bulk Submissions Tool
                                                                              , 'AP000010AAW117'  # Bulk changes
                                                                              , 'AP0000108FCZ26'  # Antilope
                                                                              , 'CR000028WQ50QW'  # Payyr
                                                                              , 'CR00001R5XEA5H'  # Billy / GC Dashboard

                                                                             -- , '0FP9H2NQBV'      # Payyr
                                                                             -- , '098SAA78BK'      # Billy / GC Dashboard
                                                                            )
                   THEN 'Internal - GC'
              WHEN salesforce_all.integration_name IN   (
                                                            'Zuora'
                                                          , 'Asperato'
                                                          , 'Junifer Systems'
                                                          , 'iRaiser'
                                                          , 'Vindicia'
                                                          )
                THEN salesforce_all.integration_name
            ELSE COALESCE(apps.name, creditors.name) -- 03-08-2018 -- not sure whether the creditor's name is actually correct here - needs more testing
         END AS partner_name


         -- new code \ 31-07-2018

         --, creditor_partner_id as partner_id
         , payment_actions.mandate_id
         , payments.organisation_id
         , payments.creditor_merchant_id AS creditor_id
         , payment_actions.created_at
         , payment_actions.updated_at
         , to_state
         , payment_actions.metadata
         , sort_key
         , payment_actions.merchant_payout_id
         , partner_payout_id
         , bank_report_entry_id
         , bank_submission_batch_id
         , CAST(NULL AS INT64) AS refund_sort_key

         -- V2 New code 01-08-2018 >> []

         , CASE WHEN payment_actions.to_state = 'paid'
                     THEN COALESCE(app_fees.amount, 0) + payments.partner_fee
                WHEN payment_actions.to_state IN (
                                                    'charged_back'
                                                  , 'late_failure'
                                                 )
                     THEN COALESCE(app_fees.amount, 0) - payments.partner_fee
                ELSE NULL
           END AS partner_fees

         , CASE WHEN payment_actions.to_state IN (
                                                    'paid'
                                                  , 'charged_back'
                                                  , 'late_failure'


                                                 )
                THEN COALESCE(app_fees.currency, payments.currency)
                ELSE NULL
           END AS partner_fees_currency

           -- V2 New code 01-08-2018 // [partner_fees]

         ------ organisation revenue ------
         , CASE WHEN payment_actions.to_state IN ('paid')
                THEN COALESCE(transaction_fees.amount, 0) + payments.transaction_fee
                WHEN payment_actions.to_state IN ('late_failure', 'charged_back')
                THEN COALESCE(transaction_fees.amount, 0) - payments.transaction_fee
                -- ^^ In the transaction_fees table, credit actions (i.e. reversed
                --    payments) have negative fees. Legacy credit actions have positive
                --    fees, so we make them negative for correct revenue summation.
                ELSE NULL
           END AS transaction_fee
         , CASE WHEN payment_actions.to_state IN ('late_failure', 'charged_back', 'paid')
                THEN COALESCE(transaction_fees.currency, payments.currency)
           END AS transaction_fee_currency

         -- revenue shares
         , CASE WHEN to_state IN ('paid')
                THEN COALESCE(revenue_shares.amount, 0) + legacy_affiliate_fee
                WHEN to_state IN ('charged_back', 'late_failure')
                THEN COALESCE(revenue_shares.amount, 0) - legacy_affiliate_fee
           END AS revenue_share
         , CASE WHEN payment_actions.to_state IN ('late_failure', 'charged_back', 'paid')
                THEN COALESCE(revenue_shares.currency, payments.currency)
           END AS revenue_share_currency

         -- app fees
         , CASE WHEN to_state IN ('paid')
                THEN COALESCE(app_fees.amount, 0) + (partner_fee - legacy_affiliate_fee)
                WHEN to_state IN ('charged_back', 'late_failure')
                THEN COALESCE(app_fees.amount, 0) - (partner_fee - legacy_affiliate_fee)
           END AS app_fee
        , CASE WHEN payment_actions.to_state IN ('late_failure', 'charged_back', 'paid')
               THEN COALESCE(app_fees.currency, payments.currency)
          END AS app_fee_currency

        -- monthly fees

        , CAST(NULL AS FLOAT64) AS monthly_fee
        , CAST(NULL AS STRING) AS monthly_fee_currency

        -- refunds
        , CAST(NULL AS FLOAT64) AS refund_amount
        , CAST(NULL AS BOOLEAN) AS refund_paid_out
        , payments.scheme
        , payments.amount   as paid_amount

     FROM `gc-data-infrastructure-7e07.gc_paysvc_live_production.payment_actions`                   payment_actions
           JOIN payment_filter payments ON payments.id = payment_actions.payment_id
           LEFT OUTER JOIN `gc-data-infrastructure-7e07.gc_paysvc_live_production.transaction_fees` transaction_fees
                           ON transaction_fees.payment_action_id = payment_actions.id
           LEFT OUTER JOIN `gc-data-infrastructure-7e07.gc_paysvc_live_production.app_fees`         app_fees
                           ON app_fees.payment_action_id = payment_actions.id
           LEFT OUTER JOIN `gc-data-infrastructure-7e07.gc_paysvc_live_production.revenue_shares`   revenue_shares
                           ON revenue_shares.transaction_fee_id = transaction_fees.id
           LEFT OUTER JOIN  `gc-data-infrastructure-7e07.import.salesforce_all`                     salesforce_all
                          ON salesforce_all.organisation_id = payments.organisation_id
           -- New Code 03-08-2018 >>
            LEFT OUTER JOIN  `gc-data-infrastructure-7e07.gc_paysvc_live_production.apps`           apps
                          ON payments.app_id = apps.id

            LEFT OUTER JOIN  `gc-data-infrastructure-7e07.gc_paysvc_live_production.creditors`      creditors
                          ON payments.creditor_partner_id = creditors.id

           -- New Code 03-08-2018 //

 ), refunds_prep AS (

    SELECT refunds.id
        , 'refunds' AS source_table
        , refunds.payment_id
        , CAST(NULL AS STRING)                   AS mandate_id
        , payments.organisation_id
        , ''                                     AS Partner_ID
        , ''                                     AS Partner_Name
        , payments.creditor_merchant_id          AS creditor_id
        , refunds.created_at
        , refunds.updated_at
        , CASE WHEN refunds.amount != refunds.payment_amount
               THEN 'partial_refund'
               ELSE 'refund'
          END                                    AS to_state
        , CAST(NULL AS STRING)                   AS metadata
        , CAST(NULL AS INT64)                    AS sort_key
        , refunds.merchant_payout_id
        , refunds.partner_payout_id
        , CAST(NULL AS STRING)                   AS bank_report_entry_id
        , CAST(NULL AS STRING)                   AS bank_submission_batch_id
        , refunds.sort_key                       AS refund_sort_key

        , 0                                      AS partner_fees
        , ''                                     AS partner_fees_currency

        , CAST(NULL AS FLOAT64)                  AS transaction_fee
        , CAST(NULL AS STRING)                   AS transaction_fee_currency
        , CAST(NULL AS FLOAT64)                  AS revenue_share
        , CAST(NULL AS STRING)                   AS revenue_share_currency
        , CAST(NULL AS FLOAT64)                  AS app_fee
        , CAST(NULL AS STRING)                   AS app_fee_currency
        , CAST(NULL AS FLOAT64)                  AS monthly_fee
        , CAST(NULL AS STRING)                   AS monthly_fee_currency
        , refunds.amount                         AS refund_amount
        , refunds.merchant_payout_id IS NOT NULL AS refund_paid_out
        , payments.scheme
     FROM `gc-data-infrastructure-7e07.gc_paysvc_live_production.refunds` refunds
          JOIN payment_filter payments ON payments.id = refunds.payment_id

), monthly_fees_prep AS (

    SELECT monthly_fees.id
         , 'monthly_fees'                        AS source_table
         , CAST(NULL AS STRING)                  AS payment_id
         , CAST(NULL AS STRING)                  AS mandate_id
         , monthly_fees.organisation_id
         , ''                                    AS Partner_ID
         , ''                                    AS Partner_Name
         , CAST(NULL AS STRING) AS creditor_id
         , CAST(monthly_fees.month AS TIMESTAMP) AS created_at
         , monthly_fees.updated_at
         , 'monthly_fee'                         AS to_state
         , CAST(NULL AS STRING)                  AS metadata
         , CAST(NULL AS INT64)                   AS sort_key
         , CAST(NULL AS STRING)                  AS merchant_payout_id
         , CAST(NULL AS STRING)                  AS partner_payout_id
         , CAST(NULL AS STRING)                  AS bank_report_entry_id
         , CAST(NULL AS STRING)                  AS bank_submission_batch_id
         , CAST(NULL AS INT64)                   AS refund_sort_key

         , 0                                     AS partner_fees
         , ''                                    AS partner_fees_currency

         , CAST(NULL AS FLOAT64)                 AS transaction_fee
         , CAST(NULL AS STRING)                  AS transaction_fee_currency
         , CAST(NULL AS FLOAT64)                 AS revenue_share
         , CAST(NULL AS STRING)                  AS revenue_share_currency
         , CAST(NULL AS FLOAT64)                 AS app_fee
         , CAST(NULL AS STRING)                  AS app_fee_currency
         , amount                                AS monthly_fee
         , currency                              AS monthly_fee_currency
         , CAST(NULL AS FLOAT64)                 AS refund_amount
         , CAST(NULL AS STRING) IS NOT NULL      AS refund_paid_out
         , monthly_fees.scheme                   AS scheme

      FROM `gc-data-infrastructure-7e07.gc_paysvc_live_production.monthly_fees` monthly_fees
     )

SELECT id
     , source_table
     , payment_id
     , organisation_id
     , partner_id
     , partner_name
     , partner_fees
     , creditor_id
     , mandate_id
     , created_at
     , updated_at
     , to_state
     , sort_key
     , refund_sort_key
     , merchant_payout_id
     , partner_payout_id
     , bank_report_entry_id
     , bank_submission_batch_id
     , transaction_fee
     , transaction_fee_currency
     , revenue_share
     , revenue_share_currency
     , app_fee
     , app_fee_currency
     , monthly_fee
     , monthly_fee_currency
     , refund_amount
     , refund_paid_out
     , scheme
     , paid_amount
     , JSON_EXTRACT_SCALAR(metadata, "$['origin']") AS origin
     , JSON_EXTRACT_SCALAR(metadata, "$['cause']") AS cause
     , JSON_EXTRACT_SCALAR(metadata, "$['parent_event_id']") AS parent_event_id
     , JSON_EXTRACT_SCALAR(metadata, "$['payout_id']") AS payout_id
     , JSON_EXTRACT_SCALAR(metadata, "$['bank_account_id']") AS bank_account_id
     , JSON_EXTRACT_SCALAR(metadata, "$['report_type']") AS report_type
     , JSON_EXTRACT_SCALAR(metadata, "$['arudd']") AS arudd
     , JSON_EXTRACT_SCALAR(metadata, "$['bacs_transaction_code']") AS bacs_transaction_code
     , JSON_EXTRACT_SCALAR(metadata, "$['reason_code']") AS reason_code
     , JSON_EXTRACT_SCALAR(metadata, "$['source_access_token_id']") AS source_access_token_id
     , JSON_EXTRACT_SCALAR(metadata, "$['paying_bank_reference']") AS paying_bank_reference
     , JSON_EXTRACT_SCALAR(metadata, "$['backfilled_on']") AS backfilled_on
 FROM  payment_actions_prep


        UNION ALL

        SELECT id
     , source_table
     , payment_id
     , organisation_id
     , partner_id
     , partner_name
     , partner_fees
     , creditor_id
     , mandate_id
     , created_at
     , updated_at
     , to_state
     , sort_key
     , refund_sort_key
     , merchant_payout_id
     , partner_payout_id
     , bank_report_entry_id
     , bank_submission_batch_id
     , transaction_fee
     , transaction_fee_currency
     , revenue_share
     , revenue_share_currency
     , app_fee
     , app_fee_currency
     , monthly_fee
     , monthly_fee_currency
     , refund_amount
     , refund_paid_out
     , scheme
     , 0 as paid_amount -- seemingly we don't use this a contra figure, but based on first go at this on 30-08-2018 producing a slightly large 18.5Bn GBP (10ish Bn GBP ballpark is more realsitic)  we may need to ?? 31-08-2018
     , JSON_EXTRACT_SCALAR(metadata, "$['origin']") AS origin
     , JSON_EXTRACT_SCALAR(metadata, "$['cause']") AS cause
     , JSON_EXTRACT_SCALAR(metadata, "$['parent_event_id']") AS parent_event_id
     , JSON_EXTRACT_SCALAR(metadata, "$['payout_id']") AS payout_id
     , JSON_EXTRACT_SCALAR(metadata, "$['bank_account_id']") AS bank_account_id
     , JSON_EXTRACT_SCALAR(metadata, "$['report_type']") AS report_type
     , JSON_EXTRACT_SCALAR(metadata, "$['arudd']") AS arudd
     , JSON_EXTRACT_SCALAR(metadata, "$['bacs_transaction_code']") AS bacs_transaction_code
     , JSON_EXTRACT_SCALAR(metadata, "$['reason_code']") AS reason_code
     , JSON_EXTRACT_SCALAR(metadata, "$['source_access_token_id']") AS source_access_token_id
     , JSON_EXTRACT_SCALAR(metadata, "$['paying_bank_reference']") AS paying_bank_reference
     , JSON_EXTRACT_SCALAR(metadata, "$['backfilled_on']") AS backfilled_on
 FROM refunds_prep


        UNION ALL
        SELECT id
     , source_table
     , payment_id
     , organisation_id
     , partner_id
     , partner_name
     , partner_fees
     , creditor_id
     , mandate_id
     , created_at
     , updated_at
     , to_state
     , sort_key
     , refund_sort_key
     , merchant_payout_id
     , partner_payout_id
     , bank_report_entry_id
     , bank_submission_batch_id
     , transaction_fee
     , transaction_fee_currency
     , revenue_share
     , revenue_share_currency
     , app_fee
     , app_fee_currency
     , monthly_fee
     , monthly_fee_currency
     , refund_amount
     , refund_paid_out
     , scheme
     , 0 as paid_amount
     , JSON_EXTRACT_SCALAR(metadata, "$['origin']") AS origin
     , JSON_EXTRACT_SCALAR(metadata, "$['cause']") AS cause
     , JSON_EXTRACT_SCALAR(metadata, "$['parent_event_id']") AS parent_event_id
     , JSON_EXTRACT_SCALAR(metadata, "$['payout_id']") AS payout_id
     , JSON_EXTRACT_SCALAR(metadata, "$['bank_account_id']") AS bank_account_id
     , JSON_EXTRACT_SCALAR(metadata, "$['report_type']") AS report_type
     , JSON_EXTRACT_SCALAR(metadata, "$['arudd']") AS arudd
     , JSON_EXTRACT_SCALAR(metadata, "$['bacs_transaction_code']") AS bacs_transaction_code
     , JSON_EXTRACT_SCALAR(metadata, "$['reason_code']") AS reason_code
     , JSON_EXTRACT_SCALAR(metadata, "$['source_access_token_id']") AS source_access_token_id
     , JSON_EXTRACT_SCALAR(metadata, "$['paying_bank_reference']") AS paying_bank_reference
     , JSON_EXTRACT_SCALAR(metadata, "$['backfilled_on']") AS backfilled_on
   FROM  monthly_fees_prep

;
