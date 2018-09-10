SELECT organisations.id AS organisation_id
     , organisations.name
     , organisations.created_at
     , creditors.signup_channel
     , creditors.signup_channel_stack
     , creditors.is_using_v2
     , creditors.has_migrated_from_v1_to_v2
     , TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), organisations.created_at, DAY) / 30 AS length_of_relationship_months
     , vertical_classification.parent_vertical
     , vertical_classification.subvertical
     -- creditor table
     , creditors.id AS creditor_id
     , creditors.geo
     , creditors.geo_source
     , creditors.organisation_with_multiple_creditors
     , creditors.merchant_type
     , creditors.monthly_collections
     , creditors.is_internal_user
     -- assume everyone starts at standard, even those who sign up for pro need to go through transition first
     -- if package transition is pending, use the current package (e.g. before the transition)
     , CASE WHEN package_transitions.to_state LIKE '%pending%' THEN SPLIT(package_transitions.to_state, '_')[OFFSET(0)]
            WHEN package_transitions.to_state IS NOT NULL THEN package_transitions.to_state
            ELSE 'standard'
       END AS package
     , COALESCE (apps.id, connect_acquisitions.app_id) AS signup_app_id
     , COALESCE (apps.name, connect_acquisitions.app_name) AS signup_app_name
     , CASE WHEN salesforce_all.partner_model IN ('Partner - Integration - Reseller', 'Partner - Integration - Pass-Through')
                 THEN 'Resellers'
            WHEN (salesforce_all.stage IS NOT NULL AND salesforce_all.stage <> 'Live - Self-Serve')
                 THEN 'Sales'
            ELSE 'Online'
       END AS sales_channel
     -- direct/connect -> BASED ON GBQ CONNECT ACQUISITIONS
     -- CAVEATS
     -- 30k+ merchants are connected to multiple partners
     -- therefore, connection to partner != revenue from partner
     -- we could incorporate payment volume for each as well?
     , connect_acquisitions.app_name
     , connect_acquisitions.connected_at
     , CASE WHEN product_connection_prep.organisation_id IS NOT NULL
            THEN product_connection_prep.connection_type
            ELSE "Unknown"
       END AS product_connection_type
     , segmentation_pre2018.segment AS segment_pre2018
     , segmentation.segment
     , payment_count_by_product.source AS primary_payment_source
     , payment_count_by_product.partner_id AS primary_partner_id
     , payment_count_by_product.partner_name AS primary_partner_name
     , scheme_summary.all_schemes
     , scheme_summary.number_schemes_connected
     , scheme_summary.primary_scheme_using_mandates
     , scheme_summary.primary_scheme
     , JSON_EXTRACT_SCALAR(signup_details, "$[referral_code]") as referral_code
     , JSON_EXTRACT_SCALAR(signup_details, "$[referral_code_stack]") as referral_code_stack

  FROM `{{ params.gbq_project_id }}.{{ params.gbq_dataset_raw_data_deduplicated }}.organisations` organisations
       LEFT JOIN `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_creditors` creditors
                  ON creditors.organisation_id = organisations.id
                     AND creditor_organisation_rank_by_created_at = 1
       LEFT JOIN `{{ params.gbq_project_id }}.{{ params.gbq_dataset_imported_data }}.salesforce_all` salesforce_all
                  USING (organisation_id)
       LEFT JOIN `{{ params.gbq_project_id }}.{{ params.gbq_dataset_raw_data_deduplicated }}.apps` apps
                 ON apps.id = organisations.signup_app_id
       LEFT JOIN `{{ params.gbq_project_id }}.{{ params.gbq_dataset_raw_data_deduplicated }}.package_transitions` package_transitions
                 ON package_transitions.organisation_id = organisations.id
                    AND package_transitions.most_recent
       LEFT JOIN `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_connect_acquisitions` connect_acquisitions
                 ON organisations.id = connect_acquisitions.organisation_id
                    AND connect_acquisitions.partner_connection_rank = 1
       LEFT JOIN `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_segmentation` segmentation_pre2018
                 ON organisations.id = segmentation_pre2018.organisation_id
       LEFT JOIN `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_payment_count_by_org_by_product` payment_count_by_product
                 ON organisations.id = payment_count_by_product.organisation_id
                    AND payment_count_by_product.rank_by_payment_count_and_last_payment = 1
       LEFT JOIN `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_scheme_summary` scheme_summary
                 ON scheme_summary.organisation_id = organisations.id
       LEFT JOIN `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_product_connection_prep` product_connection_prep
                 ON organisations.id = product_connection_prep.organisation_id
       LEFT JOIN `{{ params.gbq_project_id }}.{{ params.segmentation_dataset }}.vertical_classification`  vertical_classification
                 ON organisations.id = vertical_classification.organisation_id
       LEFT JOIN `gc-data-infrastructure-7e07.{{ params.segmentation_dataset }}.initial_segments` segmentation
                 ON organisations.id = segmentation.organisation_id
;
