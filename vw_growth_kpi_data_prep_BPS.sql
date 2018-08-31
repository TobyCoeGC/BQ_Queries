WITH activity_event_counts AS (


	              SELECT date(created_at) as kpi_day
	                   , organisation_id
	                   , SUM(CASE WHEN event_name = 'activated' THEN 1 ELSE 0 END) AS activated
	                   , SUM(CASE WHEN event_name = 'reactivated' THEN 1 ELSE 0 END) AS reactivated
	                   , SUM(CASE WHEN event_name = 'deactivated' THEN 1 ELSE 0 END) AS deactivated
	                   , SUM(CASE WHEN event_name = 'signed_up' THEN 1 ELSE 0 END) AS sign_ups
	                FROM `gc-data-infrastructure-7e07.materialized_views.vw_activity_events`
	            GROUP BY kpi_day
	                   , organisation_id


	        ), daily_states AS (


	              SELECT * FROM `gc-data-infrastructure-7e07.materialized_views.vw_kpi_daily_states`


	              UNION ALL
	              SELECT salesforce.kpi_day
	                   , salesforce.organisation_id
	                   , 'preactive' AS to_state
	                   , first_activated.created_at first_activated_At
	                FROM `gc-data-infrastructure-7e07.materialized_views.vw_salesforce_growth_kpi_prep` salesforce
	                LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_kpi_daily_states` daily_states USING(organisation_id, kpi_day)
	                LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_activity_events` first_activated
	                          ON first_activated.organisation_id = salesforce.organisation_id
	                          AND event_name = 'activated'
	               WHERE daily_states.kpi_day IS NULL


	              UNION ALL
	                SELECT salesforce.kpi_day
	                   , salesforce.organisation_id
	                   , 'preactive' AS to_state
	                   , NULL AS first_activated_At
	                FROM `gc-data-infrastructure-7e07.materialized_views.vw_salesforce_growth_kpi_prep` salesforce
	                LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_kpi_daily_states` daily_states USING(organisation_id)
	               WHERE daily_states.organisation_id IS NULL
	                 AND salesforce.kpi_day IS NOT NULL




	        ), data_prep AS (


	           SELECT DISTINCT
                   organisations.organisation_id
	               , daily_states.kpi_day
	               , daily_states.to_state
	               , organisations.name AS organisation_name

                 -- V2 columns >
                 , payment_prep.partner_id
                 , payment_prep.partner_name
                 , payment_prep.scheme as scheme
                 , payment_prep.partner_fees as partner_fees
                 , payment_prep.paid_amount  AS paid_amount
                 , Revenue_share
                 -- V2 columns \

	               , COALESCE(organisations.geo, salesforce.country_code) AS geo
	               , organisations.primary_scheme
	               , organisations.parent_vertical
	               ,
                    CASE
                        WHEN COALESCE(organisations.segment, salesforce.segment) = 'Invoicing Scale Ups'
                         OR  COALESCE(organisations.segment, salesforce.segment) =  'Traditional DD Invoicers'THEN 'Traditional DD Invoicers'
                        WHEN COALESCE(organisations.segment, salesforce.segment) = 'Small Invoicer'           THEN 'Small Invoicers'
                        WHEN COALESCE(organisations.segment, salesforce.segment) = 'Small Subscription'       THEN 'Small Subscriptions'
                        WHEN COALESCE(organisations.segment, salesforce.segment) = 'unknown'                  THEN 'Unknown'
                        WHEN COALESCE(organisations.segment, salesforce.segment) IS NULL                      THEN 'Small Invoicers'
                        ELSE COALESCE(organisations.segment, salesforce.segment)
                    END  AS Segment

	               , organisations.segment_pre2018
	               , organisations.signup_channel
	               , organisations.primary_partner_name
	               , organisations.package
	               , organisations.product_connection_type
	               , daily_states.first_activated_at
	               , organisations.created_at
	               , organisations.signup_app_id
	               --, organisations.signup_app_name
                 , CASE WHEN remap.destval IS NULL THEN organisations.signup_app_name ELSE remap.destval END AS signup_app_name
	               , COALESCE(salesforce.sales_channel, 'Self Serve') as sales_channel
	               , CASE WHEN daily_states.kpi_day = date_trunc(daily_states.kpi_day, month) THEN TRUE ELSE FALSE END AS first_day_of_month
	               , CASE WHEN daily_states.kpi_day = date_trunc(daily_states.kpi_day, quarter) THEN TRUE ELSE FALSE END AS first_day_of_quarter
	               , CASE WHEN daily_states.kpi_day = date_trunc(daily_states.kpi_day, year) THEN TRUE ELSE FALSE END AS first_day_of_year
	               , CASE WHEN daily_states.to_state = 'active' THEN 1 ELSE 0 END AS active
	               , CASE WHEN daily_states.to_state = 'inactive' THEN 1 ELSE 0 END AS inactive
	               -- activity events
	               , COALESCE(activated, 0) AS activated
	               , COALESCE(reactivated, 0) AS reactivated
	               , COALESCE(deactivated, 0) AS deactivated
	               , COALESCE(activated, 0) + COALESCE(reactivated, 0) - COALESCE(deactivated, 0) AS net_activated
	               , COALESCE(activated, 0) - (COALESCE(reactivated, 0) + COALESCE(activated, 0)) + COALESCE(deactivated) AS net_deactivated
	               , COALESCE(sign_ups, 0) AS sign_ups
	               -- mandates
	            --   , COALESCE(mandates_created, 0) AS mandates_created
	               -- payment events
	              , COALESCE(chargeback, 0) AS chargeback
	              , COALESCE(failed, 0) AS failed
	              , COALESCE(late_failure, 0) AS late_failure
	              , COALESCE(rejected, 0) AS rejected
	              , COALESCE(late_failure, 0) + COALESCE(failed, 0) + COALESCE(rejected, 0) AS total_failed_payments
	              , COALESCE(submitted, 0) AS submitted
	              , CASE WHEN payment_prep.to_state = 'active' THEN COALESCE(paid, 0) ELSE 0 END AS paid_active
	              , CASE WHEN payment_prep.to_state = 'inactive' THEN COALESCE(paid, 0) ELSE 0 END AS paid_inactive
	              , CASE WHEN payment_prep.to_state = 'preactive' THEN COALESCE(paid, 0) ELSE 0 END AS paid_preactive

	                 -- active revenue events
	               , CASE WHEN revenue_prep.to_state = 'active' THEN COALESCE(transaction_fee, 0) ELSE 0 END AS transaction_fee_active
	         --      , CASE WHEN revenue_prep.to_state = 'active' THEN COALESCE(monthly_fee, 0) ELSE 0 END AS monthly_fee_active
/*
	               , CASE WHEN COALESCE(deactivated, 0) = 1 THEN
	                 SUM(CASE WHEN revenue_prep.to_state = 'active' THEN COALESCE(transaction_fee, 0) + COALESCE(monthly_fee, 0) ELSE 0 END)
	                 OVER (PARTITION BY organisation_id  ORDER BY daily_states.kpi_day ROWS BETWEEN 130 PRECEDING AND 40 PRECEDING) ELSE 0 END AS gross_active_mrr_pre_deactivation
*/
	               -- inactive revenue events
	               , CASE WHEN revenue_prep.to_state = 'inactive' THEN COALESCE(transaction_fee, 0) ELSE 0 END AS transaction_fee_inactive
	           --    , CASE WHEN revenue_prep.to_state = 'inactive' THEN COALESCE(monthly_fee, 0) ELSE 0 END AS monthly_fee_inactive
	               -- preactive revenue events
	               , CASE WHEN revenue_prep.to_state = 'preactive'  THEN COALESCE(transaction_fee, 0) ELSE 0 END AS transaction_fee_preactive
	             --  , CASE WHEN revenue_prep.to_state = 'preactive' OR revenue_prep.to_state IS NULL THEN COALESCE(monthly_fee, 0) ELSE 0 END AS monthly_fee_preactive
	               -- 90 days after activation metrics
	               -- active mrr
                
                 /*
	              , CASE WHEN DATE_DIFF(daily_states.kpi_day, DATE(first_activated_at), day)+1 = 90 THEN
	                SUM(CASE WHEN revenue_prep.to_state = 'active' THEN COALESCE(transaction_fee, 0) + COALESCE(monthly_fee, 0) ELSE 0 END)
	                OVER (PARTITION BY organisation_id ORDER BY daily_states.kpi_day ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) ELSE 0 END AS active_mrr_90_days_after_activation

                 -- payments

	              , CASE WHEN DATE_DIFF(daily_states.kpi_day, DATE(first_activated_at), day)+1 = 90  THEN
	                SUM(CASE WHEN timestamp(daily_states.kpi_day)>= first_activated_at AND payment_prep.to_state = 'active' THEN COALESCE(paid, 0)  ELSE 0 END)
	                OVER (PARTITION BY organisation_id ORDER BY daily_states.kpi_day ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) ELSE 0 END AS active_payments_90_days_after_activation
	               -- 90 day activation flag
	              , CASE WHEN DATE_DIFF(daily_states.kpi_day, DATE(first_activated_at), day)+1 = 90 AND daily_states.to_state = 'active' THEN 1 ELSE 0 END AS merchant_active_90_days_after_activation
                */
	              -- salesforce data
	              , salesforce.lead_id
	              , salesforce.opportunity_id
	              , salesforce.source_bucket
	              , salesforce.opportunity_type
	              , salesforce.lead_created_date
	              , salesforce.lead_status
	              , salesforce.lead_addressability
	              , salesforce.opportunity_created_date
	              , salesforce.opportunity_stage_name
	              , salesforce.opportunity_stage_number
	              , salesforce.opportunity_package
	              , salesforce.opportunity_ramp_up_method
	              , salesforce.opportunity_won
	              , salesforce.opportunity_closed
	              , salesforce.product
	              , salesforce.team
	              , salesforce.opportunity_closed_by
	              , salesforce.acv
	              , salesforce.tcv
	              , salesforce.channel
	              , salesforce.country as salesforce_country
	              , salesforce.country_code as salesforce_country_code
	              , salesforce.contract_length_years
	              , salesforce.lead_flag
	              , salesforce.opportunity_flag
	              , salesforce.contract_length_months
	              , salesforce.opportunity_contract_signed_date
	              , salesforce.opportunity_closed_date
	              , salesforce.opportunity_closed_lost_date
	              , salesforce.organisation_id as salesforce_organisation_id
	              , salesforce.merchant_partner
	              , salesforce.lead_to_opportunity_days
	              , salesforce.lead_to_opportunity_hours
	              , salesforce.source
	              , salesforce.salesforce_currency

                , EXTRACT(MONTH from daily_states.kpi_day)               AS Mat_Month
                , EXTRACT(QUARTER FROM daily_states.kpi_day)             AS Mat_Quarter
                , EXTRACT(YEAR FROM daily_states.kpi_day)                AS Mat_Year
                , DATE_TRUNC(DATE(organisations.created_at), month)      AS Cohort

	          FROM daily_states
	               LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_organisations` organisations USING (organisation_id)
	               LEFT JOIN activity_event_counts USING (organisation_id, kpi_day)
	              -- LEFT JOIN `gc-data-infrastructure-7e07.experimental_tables.vw_kpi_mandates_prep` mandates_prep
	                       --  USING (organisation_id, kpi_day)

                 LEFT JOIN `gc-data-infrastructure-7e07.experimental_tables.vw_kpi_payment_prep` payment_prep
	                         USING (organisation_id, kpi_day)

	               LEFT JOIN `gc-data-infrastructure-7e07.experimental_tables.vw_kpi_revenue_prep` revenue_prep
	                         USING (organisation_id, kpi_day, partner_id, scheme)
	               LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_salesforce_growth_kpi_prep` salesforce
	                         USING (organisation_id, kpi_day)
                 LEFT JOIN  `gc-data-infrastructure-7e07.experimental_tables.App_Name_Remap_Lookup`  remap
                           ON  organisations.signup_app_name= RTRIM(remap.SourceVal)
	        )

,  pre_prep AS
            (
	          SELECT data_prep.* EXCEPT (first_day_of_month,
	                                     first_day_of_quarter,
	                                     first_day_of_year,
	                                     net_activated,
                                       Mat_Month,
	                                     to_state
                                       )

                 , CASE
                        WHEN Mat_Month = 1 THEN 'Jan'
                        WHEN Mat_Month = 2 THEN 'Feb'
                        WHEN Mat_Month = 3 THEN 'Mar'
                        WHEN Mat_Month = 4 THEN 'Apr'
                        WHEN Mat_Month = 5 THEN 'May'
                        WHEN Mat_Month = 6 THEN 'Jun'
                        WHEN Mat_Month = 7 THEN 'Jul'
                        WHEN Mat_Month = 8 THEN 'Aug'
                        WHEN Mat_Month = 9 THEN 'Sep'
                        WHEN Mat_Month = 10 THEN 'Oct'
                        WHEN Mat_Month = 11 THEN 'Nov'
                        WHEN Mat_Month = 12 THEN 'Dec'
                    END                                          AS Mat_Month

	               -- ACTIVE CALCS
	               , CASE WHEN first_day_of_month THEN COALESCE(active, 0) ELSE COALESCE(net_activated, 0) END AS active_monthly
	               , CASE WHEN first_day_of_quarter THEN COALESCE(active, 0) ELSE COALESCE(net_activated, 0) END AS active_quarterly
	               , CASE WHEN first_day_of_year THEN COALESCE(active, 0) ELSE COALESCE(net_activated, 0) END AS active_yearly
	               -- INACTIVE CALCS
	               , CASE WHEN first_day_of_month THEN COALESCE(inactive, 0) ELSE COALESCE(net_deactivated, 0) END AS inactive_monthly
	               , CASE WHEN first_day_of_quarter THEN COALESCE(inactive, 0) ELSE COALESCE(net_deactivated, 0) END AS inactive_quarterly
	               , CASE WHEN first_day_of_year THEN COALESCE(inactive, 0) ELSE COALESCE(net_deactivated, 0) END AS inactive_yearly
	               , case when created_at is null then 0 else 1 end as organisation_id_validation
	               -- OPPORTUNITY TO ACTIVE CALCS
	               , timestamp_diff(first_activated_at, opportunity_created_date, day) AS opportunity_to_active_days
	               , timestamp_diff(first_activated_at, opportunity_created_date, hour) AS opportunity_to_active_hours
                 , ROW_NUMBER () OVER (PARTITION BY kpi_day, organisation_id ORDER BY scheme) AS DoubleCount_Fix


	            FROM data_prep
            )




,partner_split_on_common_scheme AS
                                    (

                                        SELECT
                                          organisation_id
                                        , kpi_day
                                        , scheme
                                        , COUNT(*)         AS PartnerDivider
                                        FROM
                                        pre_prep
                                        GROUP BY
                                          organisation_id
                                        , kpi_day
                                        , scheme

                                    )
, MF_Revenue_Prep_CTE AS
                          (

                          SELECT
                            sum(monthly_fee) as monthly_fee
                          , kpi_day
                          , organisation_id
                          , to_state
                          FROM `gc-data-infrastructure-7e07.experimental_tables.vw_kpi_revenue_prep`
                          WHERE EXTRACT(day from kpi_day)=1
                          GROUP BY
                           kpi_day
                          , organisation_id
                          , to_state
                          )

, pre_final_prep AS (
                      SELECT
                        base.*
                      , CASE WHEN to_state = 'preactive' THEN COALESCE(mf.monthly_fee, 0) ELSE 0 END AS monthly_fee_preactive
                      , CASE WHEN to_state = 'active' THEN COALESCE(mf.monthly_fee, 0)/coalesce(mpss.partnerDivider,1) ELSE 0 END AS monthly_fee_active
                      , CASE WHEN to_state = 'inactive' THEN COALESCE(mf.monthly_fee, 0) ELSE 0 END AS monthly_fee_inactive
                      , mp.mandates_created/mpss.PartnerDivider as Mandates_Created

                      FROM
                      pre_prep   base
                      LEFT JOIN
                      MF_Revenue_Prep_CTE  mf
                      USING (organisation_id, kpi_day)

                      LEFT JOIN
                      `gc-data-infrastructure-7e07.experimental_tables.vw_kpi_mandates_prep`                 mp
                      USING (organisation_id, kpi_day,scheme)

                      LEFT JOIN
                      partner_split_on_common_scheme                                                         mpss
                      USING (organisation_id, kpi_day,scheme)
                    )

, final_mandate_prep AS (
                        SELECT
                          pfp.* EXCEPT (active, inactive, activated, partner_name, paid_amount)
                        , COALESCE(pfp.monthly_fee_preactive,0)
                        + COALESCE(pfp.monthly_fee_active,0)
                        + COALESCE(pfp.monthly_fee_inactive,0)
                        + COALESCE(pfp.transaction_fee_preactive,0)
                        + COALESCE(pfp.transaction_fee_active,0)
                        + COALESCE(pfp.transaction_fee_inactive,0)                                            AS TotalGrossMRR
                        , CASE WHEN DoubleCount_Fix=1         THEN pfp.active           ELSE 0 END            AS active
                        , CASE WHEN DoubleCount_Fix=1         THEN pfp.inactive         ELSE 0 END            AS inactive
                        , CASE WHEN DoubleCount_Fix=1         THEN pfp.activated        ELSE 0 END            AS activated
                        , CASE WHEN DoubleCount_Fix=1         THEN pfp.paid_amount      ELSE 0  END           AS paid_amount
                        , CASE WHEN Remap.DestVal IS NOT NULL THEN Remap.DestVal        ELSE Partner_name END AS Partner_name_rollup -- to allow a grouped 'partner_name rollup' dimension or partner name low level dimension
                        FROM
                        pre_final_prep AS pfp
                          LEFT JOIN
                        `gc-data-infrastructure-7e07.experimental_tables.App_Name_Remap_Lookup`  AS Remap
                         ON
                        pfp.Partner_name = RTRIM(Remap.SourceVal)

                      )

 , final_mandate_fix AS(
                          SELECT
                           fp.* EXCEPT (Mandates_created, monthly_fee_preactive, monthly_fee_active, monthly_fee_inactive, TotalGrossMRR, scheme, deactivated, net_deactivated, reactivated, activated, active, doublecount_fix)
                         , mp.mandates_created
                         , mp.scheme                             AS scheme
                         , 0                                     AS monthly_fee_preactive
                         , 0                                     AS monthly_fee_active
                         , 0                                     AS monthly_fee_inactive
                         , 0                                     AS TotalGrossMRR
                         , 0                                     AS deactivated
                         , 0                                     AS net_deactivated
                         , 0                                     AS reactivated
                         , 0                                     AS activated
                         , 0                                     AS active
                         , 1                                     AS mandateSchemeFix
                         , 0                                     AS doublecount_fix


                         FROM
                         final_mandate_prep                                                                     fp
                         JOIN
                         `gc-data-infrastructure-7e07.experimental_tables.vw_kpi_mandates_prep`                 mp
                         USING (organisation_id, kpi_day)
                         WHERE
                         fp.scheme            IS NULL
                         AND
                         fp.mandates_created  IS NULL

                         UNION DISTINCT

                         SELECT
                           fp.* EXCEPT (Mandates_created, monthly_fee_preactive, monthly_fee_active, monthly_fee_inactive, TotalGrossMRR, scheme, deactivated, net_deactivated, reactivated, activated, active, doublecount_fix)
                         , fp.mandates_created
                         , fp.scheme
                         , fp.monthly_fee_preactive
                         , fp.monthly_fee_active
                         , fp.monthly_fee_inactive
                         , fp.totalgrossMRR
                         , fp.deactivated
                         , fp.net_deactivated
                         , fp.reactivated
                         , fp.activated
                         , fp.active
                         , 0                                     AS mandateSchemeFix
                         , fp.doublecount_fix

                         FROM
                         final_mandate_prep                                                                             fp
                        )
, fix_90 AS ( SELECT
               organisation_id
             , kpi_day


            , CASE WHEN DATE_DIFF(daily_states.kpi_day, DATE(first_activated_at), day)+1 = 90  THEN
                              SUM(CASE WHEN timestamp(daily_states.kpi_day)>= first_activated_at AND payment_prep.to_state = 'active' THEN COALESCE(paid, 0)  ELSE 0 END)
                              OVER (PARTITION BY organisation_id ORDER BY daily_states.kpi_day ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) ELSE 0 END AS active_payments_90_days_after_activation
            , CASE WHEN DATE_DIFF(daily_states.kpi_day, DATE(first_activated_at), day)+1 = 90 AND daily_states.to_state = 'active' THEN 1 ELSE 0 END AS merchant_active_90_days_after_activation
            , CASE WHEN COALESCE(deactivated, 0) = 1 THEN
	                 SUM(CASE WHEN revenue_prep.to_state = 'active' THEN COALESCE(transaction_fee, 0) + COALESCE(monthly_fee, 0) ELSE 0 END)
	                 OVER (PARTITION BY organisation_id  ORDER BY daily_states.kpi_day ROWS BETWEEN 130 PRECEDING AND 40 PRECEDING) ELSE 0 END AS gross_active_mrr_pre_deactivation

            , CASE WHEN DATE_DIFF(daily_states.kpi_day, DATE(first_activated_at), day)+1 = 90 THEN
	                SUM(CASE WHEN revenue_prep.to_state = 'active' THEN COALESCE(transaction_fee, 0) + COALESCE(monthly_fee, 0) ELSE 0 END)
	                OVER (PARTITION BY organisation_id ORDER BY daily_states.kpi_day ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) ELSE 0 END AS active_mrr_90_days_after_activation
                               FROM daily_states
                             LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_organisations` organisations USING (organisation_id)
                             LEFT JOIN activity_event_counts USING (organisation_id, kpi_day)

                             LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_kpi_payment_prep` payment_prep
                                       USING (organisation_id, kpi_day)

                             LEFT JOIN `gc-data-infrastructure-7e07.materialized_views.vw_kpi_revenue_prep` revenue_prep
                                       USING (organisation_id, kpi_day)
              )



SELECT mfm.*

, CASE WHEN doublecount_fix =1 THEN f90.active_payments_90_days_after_activation ELSE 0 END AS active_payments_90_days_after_activation
, CASE WHEN doublecount_fix =1 THEN f90.merchant_active_90_days_after_activation ELSE 0 END AS merchant_active_90_days_after_activation
, CASE WHEN doublecount_fix =1 THEN f90.gross_active_mrr_pre_deactivation ELSE 0 END AS gross_active_mrr_pre_deactivation
, CASE WHEN doublecount_fix =1 THEN f90.active_mrr_90_days_after_activation ELSE 0 END AS active_mrr_90_days_after_activation

FROM
final_mandate_fix   mfm
LEFT JOIN
fix_90              f90

 USING (organisation_id, kpi_day)
;
