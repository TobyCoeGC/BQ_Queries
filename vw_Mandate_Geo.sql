      SELECT
        date(base.created_at)       AS KPI_Day
      , basegeo.geo                 AS MerchantGeo
      , collect.country_code        AS MandateGeo
      , count(base.organisation_id) AS NumberOfPaymentsCollected
      , base.organisation_id
      , base.scheme
      , base.partner_id
      , base.partner_name
      , CASE 
            WHEN pnru.parent_app_name IS NULL
                 THEN base.partner_name
             ELSE pnru.parent_app_name
        END
                                                  AS Partner_Name_RollUp

      FROM 
      `gc-data-infrastructure-7e07.experimental_tables.vw_payment_actions_and_monthly_fees`  base 
      JOIN
      `gc-data-infrastructure-7e07.materialized_views.vw_organisations`                     basegeo
      ON base.organisation_id = basegeo.organisation_id
      JOIN
      `gc-data-infrastructure-7e07.materialized_views.vw_mandates`                          collect
      ON base.mandate_id = collect.id
      LEFT JOIN     
      `gc-data-infrastructure-7e07.experimental_tables.segmentation_mapping_partners`      pnru
      ON base.partner_name = pnru.child_app_name

      GROUP BY
        basegeo.geo
      , collect.country_code
      , date(base.created_at)
      , base.organisation_id
      , base.scheme
      , base.partner_id
      , base.partner_name
      , pnru.parent_app_name
