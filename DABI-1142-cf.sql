-- Compare revenue between dev and prod for February 2025
 -- we should expect to seen an increase in revenue for contract 210 as this is the
 -- contract associated with the new revenue source
WITH 
dev AS (
SELECT 
    COALESCE(id::TEXT, 'N/A') AS id,
    COALESCE(data_month::DATE, '2010-01-01') AS data_month,
    COALESCE(business_unit_name::TEXT, 'N/A') AS business_unit_name,
    COALESCE(business_unit_detail_name::TEXT, 'N/A') AS business_unit_detail_name,
    COALESCE(product_line_id::TEXT, 'N/A') AS product_line_id,
    COALESCE(product_line_name::TEXT, 'N/A') AS product_line_name,
    COALESCE(parent_partner_name::TEXT, 'N/A') AS parent_partner_name,
    COALESCE(partner_id::TEXT, 'N/A') AS partner_id,
    COALESCE(partner_name::TEXT, 'N/A') AS partner_name,
    COALESCE(drid::TEXT, 'N/A') AS drid,
    COALESCE(contract_id::TEXT, 'N/A') AS contract_id,
    COALESCE(contract_name::TEXT, 'N/A') AS contract_name,
    COALESCE(gam_advertiser_level1::TEXT, 'N/A') AS gam_advertiser_level1,
    COALESCE(gam_advertiser_level2::TEXT, 'N/A') AS gam_advertiser_level2,
    COALESCE(network_name_id::TEXT, 'N/A') AS network_name_id,
    COALESCE(network_name::TEXT, 'N/A') AS network_name,
    COALESCE(network_classification_name::TEXT, 'N/A') AS network_classification_name,
    COALESCE(network_classification_subtype::TEXT, 'N/A') AS network_classification_subtype,
    COALESCE(network_feed_id::TEXT, 'N/A') AS network_feed_id,
    SUM(parent_partner_id) AS parent_partner_id,
    SUM(markup_percent) AS markup_percent,
    SUM(net_percent) AS net_percent,
    SUM(revshare_percent) AS revshare_percent,
    SUM(bad_debt_percent) AS bad_debt_percent,
    SUM(management_percent) AS management_percent,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(network_gross_revenue) AS network_gross_revenue,
    SUM(network_revshare) AS network_revshare,
    SUM(s1_gross_revenue) AS s1_gross_revenue,
    SUM(managment_fee) AS managment_fee,
    SUM(partner_revshare) AS partner_revshare,
    SUM(holdback_revenue) AS holdback_revenue,
    SUM(partner_gross_revenue) AS partner_gross_revenue,
    SUM(commission_amount) AS commission_amount,
FROM BI_DEV.daron.view_partner_finance_revenue_aggregation
WHERE 1=1
    AND data_month > '2025-01-01'
    AND data_month < '2025-03-01'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
),

prod AS (
SELECT 
    COALESCE(id::TEXT, 'N/A') AS id,
    COALESCE(data_month::DATE, '2010-01-01') AS data_month,
    COALESCE(business_unit_name::TEXT, 'N/A') AS business_unit_name,
    COALESCE(business_unit_detail_name::TEXT, 'N/A') AS business_unit_detail_name,
    COALESCE(product_line_id::TEXT, 'N/A') AS product_line_id,
    COALESCE(product_line_name::TEXT, 'N/A') AS product_line_name,
    COALESCE(parent_partner_name::TEXT, 'N/A') AS parent_partner_name,
    COALESCE(partner_id::TEXT, 'N/A') AS partner_id,
    COALESCE(partner_name::TEXT, 'N/A') AS partner_name,
    COALESCE(drid::TEXT, 'N/A') AS drid,
    COALESCE(contract_id::TEXT, 'N/A') AS contract_id,
    COALESCE(contract_name::TEXT, 'N/A') AS contract_name,
    COALESCE(gam_advertiser_level1::TEXT, 'N/A') AS gam_advertiser_level1,
    COALESCE(gam_advertiser_level2::TEXT, 'N/A') AS gam_advertiser_level2,
    COALESCE(network_name_id::TEXT, 'N/A') AS network_name_id,
    COALESCE(network_name::TEXT, 'N/A') AS network_name,
    COALESCE(network_classification_name::TEXT, 'N/A') AS network_classification_name,
    COALESCE(network_classification_subtype::TEXT, 'N/A') AS network_classification_subtype,
    COALESCE(network_feed_id::TEXT, 'N/A') AS network_feed_id,
    SUM(parent_partner_id) AS parent_partner_id,
    SUM(markup_percent) AS markup_percent,
    SUM(net_percent) AS net_percent,
    SUM(revshare_percent) AS revshare_percent,
    SUM(bad_debt_percent) AS bad_debt_percent,
    SUM(management_percent) AS management_percent,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(network_gross_revenue) AS network_gross_revenue,
    SUM(network_revshare) AS network_revshare,
    SUM(s1_gross_revenue) AS s1_gross_revenue,
    SUM(managment_fee) AS managment_fee,
    SUM(partner_revshare) AS partner_revshare,
    SUM(holdback_revenue) AS holdback_revenue,
    SUM(partner_gross_revenue) AS partner_gross_revenue,
    SUM(commission_amount) AS commission_amount,
FROM bi.partner_finance.view_partner_finance_revenue_aggregation
WHERE 1=1
    AND data_month > '2025-01-01'
    AND data_month < '2025-03-01'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)

SELECT
    dev.id IS NOT NULL AS dev_records_present,
    prod.id IS NOT NULL AS prod_records_present,
    COALESCE(dev.id, prod.id) AS id,
    COALESCE(dev.data_month, prod.data_month) AS data_month,
    COALESCE(dev.business_unit_name, prod.business_unit_name) AS business_unit_name,
    COALESCE(dev.business_unit_detail_name, prod.business_unit_detail_name) AS business_unit_detail_name,
    COALESCE(dev.product_line_id, prod.product_line_id) AS product_line_id,
    COALESCE(dev.product_line_name, prod.product_line_name) AS product_line_name,
    COALESCE(dev.parent_partner_name, prod.parent_partner_name) AS parent_partner_name,
    COALESCE(dev.partner_id, prod.partner_id) AS partner_id,
    COALESCE(dev.partner_name, prod.partner_name) AS partner_name,
    COALESCE(dev.drid, prod.drid) AS drid,
    COALESCE(dev.contract_id, prod.contract_id) AS contract_id,
    COALESCE(dev.contract_name, prod.contract_name) AS contract_name,
    COALESCE(dev.gam_advertiser_level1, prod.gam_advertiser_level1) AS gam_advertiser_level1,
    COALESCE(dev.gam_advertiser_level2, prod.gam_advertiser_level2) AS gam_advertiser_level2,
    COALESCE(dev.network_name_id, prod.network_name_id) AS network_name_id,
    COALESCE(dev.network_name, prod.network_name) AS network_name,
    COALESCE(dev.network_classification_name, prod.network_classification_name) AS network_classification_name,
    COALESCE(dev.network_classification_subtype, prod.network_classification_subtype) AS network_classification_subtype,
    COALESCE(dev.network_feed_id, prod.network_feed_id) AS network_feed_id,
    COALESCE(dev.parent_partner_id, 0) AS dev_parent_partner_id,
    COALESCE(prod.parent_partner_id, 0) AS prod_parent_partner_id,
    COALESCE(dev.markup_percent, 0) AS dev_markup_percent,
    COALESCE(prod.markup_percent, 0) AS prod_markup_percent,
    COALESCE(dev.net_percent, 0) AS dev_net_percent,
    COALESCE(prod.net_percent, 0) AS prod_net_percent,
    COALESCE(dev.revshare_percent, 0) AS dev_revshare_percent,
    COALESCE(prod.revshare_percent, 0) AS prod_revshare_percent,
    COALESCE(dev.bad_debt_percent, 0) AS dev_bad_debt_percent,
    COALESCE(prod.bad_debt_percent, 0) AS prod_bad_debt_percent,
    COALESCE(dev.management_percent, 0) AS dev_management_percent,
    COALESCE(prod.management_percent, 0) AS prod_management_percent,
    COALESCE(dev.impressions, 0) AS dev_impressions,
    COALESCE(prod.impressions, 0) AS prod_impressions,
    COALESCE(dev.clicks, 0) AS dev_clicks,
    COALESCE(prod.clicks, 0) AS prod_clicks,
    COALESCE(dev.network_gross_revenue, 0) AS dev_network_gross_revenue,
    COALESCE(prod.network_gross_revenue, 0) AS prod_network_gross_revenue,
    COALESCE(dev.network_revshare, 0) AS dev_network_revshare,
    COALESCE(prod.network_revshare, 0) AS prod_network_revshare,
    COALESCE(dev.s1_gross_revenue, 0) AS dev_s1_gross_revenue,
    COALESCE(prod.s1_gross_revenue, 0) AS prod_s1_gross_revenue,
    COALESCE(dev.managment_fee, 0) AS dev_managment_fee,
    COALESCE(prod.managment_fee, 0) AS prod_managment_fee,
    COALESCE(dev.partner_revshare, 0) AS dev_partner_revshare,
    COALESCE(prod.partner_revshare, 0) AS prod_partner_revshare,
    COALESCE(dev.holdback_revenue, 0) AS dev_holdback_revenue,
    COALESCE(prod.holdback_revenue, 0) AS prod_holdback_revenue,
    COALESCE(dev.partner_gross_revenue, 0) AS dev_partner_gross_revenue,
    COALESCE(prod.partner_gross_revenue, 0) AS prod_partner_gross_revenue,
    COALESCE(dev.commission_amount, 0) AS dev_commission_amount,
    COALESCE(prod.commission_amount, 0) AS prod_commission_amount,
FROM dev FULL OUTER JOIN prod
    ON dev.id = prod.id
    AND dev.data_month = prod.data_month
    AND dev.business_unit_name = prod.business_unit_name
    AND dev.business_unit_detail_name = prod.business_unit_detail_name
    AND dev.product_line_id = prod.product_line_id
    AND dev.product_line_name = prod.product_line_name
    AND dev.parent_partner_name = prod.parent_partner_name
    AND dev.partner_id = prod.partner_id
    AND dev.partner_name = prod.partner_name
    AND dev.drid = prod.drid
    AND dev.contract_id = prod.contract_id
    AND dev.contract_name = prod.contract_name
    AND dev.gam_advertiser_level1 = prod.gam_advertiser_level1
    AND dev.gam_advertiser_level2 = prod.gam_advertiser_level2
    AND dev.network_name_id = prod.network_name_id
    AND dev.network_name = prod.network_name
    AND dev.network_classification_name = prod.network_classification_name
    AND dev.network_classification_subtype = prod.network_classification_subtype
    AND dev.network_feed_id = prod.network_feed_id
WHERE dev.id IS NULL OR prod.id IS NULL
    OR ABS(dev_parent_partner_id - prod_parent_partner_id) > 0.01
    OR ABS(dev_markup_percent - prod_markup_percent) > 0.01
    OR ABS(dev_net_percent - prod_net_percent) > 0.01
    OR ABS(dev_revshare_percent - prod_revshare_percent) > 0.01
    OR ABS(dev_bad_debt_percent - prod_bad_debt_percent) > 0.01
    OR ABS(dev_management_percent - prod_management_percent) > 0.01
    OR ABS(dev_impressions - prod_impressions) > 0.01
    OR ABS(dev_clicks - prod_clicks) > 0.01
    OR ABS(dev_network_gross_revenue - prod_network_gross_revenue) > 0.01
    OR ABS(dev_network_revshare - prod_network_revshare) > 0.01
    OR ABS(dev_s1_gross_revenue - prod_s1_gross_revenue) > 0.01
    OR ABS(dev_managment_fee - prod_managment_fee) > 0.01
    OR ABS(dev_partner_revshare - prod_partner_revshare) > 0.01
    OR ABS(dev_holdback_revenue - prod_holdback_revenue) > 0.01
    OR ABS(dev_partner_gross_revenue - prod_partner_gross_revenue) > 0.01
    OR ABS(dev_commission_amount - prod_commission_amount) > 0.01
ORDER BY data_month;


-- February network gross revenue - Contract 210
 -- When comparing dev & prod, we observe an increase in revenue for contract 210 of 24043.93246876
SELECT 1998837.59361088 - 1974793.66114212; -- 24043.93246876


-- Let's validate that all data from the new data source is making it into the view.
 -- below is a portion of the compiled SQL for the new partner_finance_sellside_gsheet_manual_revenue model
 -- we can comment out portions of the `revsource` CTE to observe how much revenue there is from each data source.
 -- We observe that the value of the old data source is the revenue amount that appears in prod.
 -- We observe that the value of the new data source is the difference in revenue between dev and prod.
 -- We observe the sum of the two values is the total revenue amount in dev.

-- 1974793.66114212 -- old data source (__dbt__cte__couponfollow_share_revenue_agg_revenue_information_by_partner)
-- 24043.932468758 -- new data source (partner_finance_additional_coupon_follow_revenue)
-- 1998837.59361088 -- combined
WITH  __dbt__cte__couponfollow_share_revenue_agg_revenue_information_by_partner_new as (

SELECT
	CASE
		WHEN partner_website = 'HowStuffWorks'
			THEN 'bi-added-couponfollow-howstuffworks-revenue'
		WHEN partner_website = 'CouponFollow'
			THEN 'bi-added-couponfollow-manual-revenue'
		WHEN partner_website = 'Cently'
			THEN 'bi-added-couponfollow-cently-revenue'
		ELSE 'bi-added-couponfollow-partner'
	END              AS account_id,
	order_date::DATE AS data_date,
	total_revenue    AS revenue,
	order_count      AS conversions,
	partner_website
FROM COUPONFOLLOW_SHARE.REVENUE.agg_revenue_information_by_partner_new
),  __dbt__cte__couponfollow_share_revenue_agg_revenue_information_new as (


SELECT
	order_date::DATE AS data_date,
	total_revenue    AS revenue
FROM COUPONFOLLOW_SHARE.REVENUE.agg_revenue_information_new
),  __dbt__cte__couponfollow_share_revenue_agg_revenue_information_by_partner as (


SELECT
	CASE
		WHEN partner_website = 'HowStuffWorks'
			THEN 'bi-added-couponfollow-howstuffworks-revenue'
		WHEN partner_website = 'CouponFollow'
			THEN 'bi-added-couponfollow-manual-revenue'
		WHEN partner_website = 'Cently'
			THEN 'bi-added-couponfollow-cently-revenue'
		ELSE 'bi-added-couponfollow-partner'
	END               AS account_id,
	order_date::DATE  AS data_date,
	revenue           AS revenue,
	order_count       AS conversions,
	partner_website
FROM COUPONFOLLOW_SHARE.REVENUE.agg_revenue_information_by_partner
),  __dbt__cte__bi_common_account_metadata_mappings as (


SELECT
	account_id,
	account_name,
	product_line_id,
	bi_account_id::INT         AS bi_account_id,
	provider_id,
	network_name_id::INT       AS network_name_id,
	network_type_id::INT       AS network_type_id,
	network_classification_subtype_id,
	account_population_method_subtype_id,
	currency_code::VARCHAR(10) AS currency_code,
	include_in_exec_aggregation,
	product_line_id_allow_update,
	effective_from,
	effective_to,
	inserted_ts,
	updated_ts
FROM BI.COMMON.account_metadata_mappings
), partner_revenue AS (
	SELECT
		data_date,
		SUM(revenue) AS revenue
	FROM __dbt__cte__couponfollow_share_revenue_agg_revenue_information_by_partner_new
	WHERE data_date >= '2024-12-01'
	group by 1
),

all_revenue AS (
	SELECT
		data_date,
		SUM(revenue) AS revenue
	FROM __dbt__cte__couponfollow_share_revenue_agg_revenue_information_new
	WHERE data_date >= '2024-12-01'
	group by 1
),

partner_finance_additional_coupon_follow_revenue AS (
	SELECT
		a.data_date,
		'bi-added-couponfollow-manual-revenue' AS account_id,
		a.revenue - p.revenue                  AS revenue
	FROM all_revenue AS a
	INNER JOIN partner_revenue AS p
		ON a.data_date = p.data_date
),

revsource AS (
	SELECT
		'bi-added-couponfollow-manual-revenue' AS account_id,
		data_date,
		revenue,
		conversions
	FROM __dbt__cte__couponfollow_share_revenue_agg_revenue_information_by_partner
	WHERE
		partner_website = 'CouponFollow'
		AND data_date != '2023-08-16'
	UNION
	SELECT
		account_id,
		data_date,
		revenue,
		0         AS conversions
	FROM partner_finance_additional_coupon_follow_revenue
),

partner_finance_sellside_manual_couponfollow_ngs_revenue AS (
	SELECT
		cfr.data_date                   AS data_ts,
		amm.product_line_id,
		amm.network_name_id,
		sc.contract_id,
		4                               AS device_type_id,	  --unknown
		3                               AS method_subtype_id, --manual
		amm.currency_code,
		1                               AS conversion_rate,
		NULL::FLOAT                     AS bidded_searches,
		NULL::FLOAT                     AS clicks,
		NULL::FLOAT                     AS impressions,
		cfr.revenue::FLOAT              AS gross_revenue,
		cfr.conversions,
		'partner_finance_sellside_gsheet_manual_revenue'::TEXT   AS aggregation_source,
		CURRENT_TIMESTAMP               AS aggregation_date
	FROM revsource AS cfr
	INNER JOIN __dbt__cte__bi_common_account_metadata_mappings AS amm
		ON cfr.account_id = amm.account_id
	INNER JOIN BI.COMMON.sellside_contracts AS sc
		ON amm.bi_account_id = sc.bi_account_id
	WHERE
		cfr.data_date >= amm.effective_from
		AND cfr.data_date <= CURRENT_DATE - 1
)--,

select sum(revenue)from revsource
where date_trunc('month', data_date) = '2025-02-01'
and conversions >= 0;
order by 1,2,3,4;

select 18097 + 2109866;

SELECT date_trunc('month', data_date) AS data_month, SUM(revenue) AS revenue
-- FROM partner_finance_sellside_manual_couponfollow_ngs_revenue
FROM revsource
WHERE data_month = '2025-01-01'
GROUP BY 1
ORDER BY 1 ASC;

select * from bi.partner_finance.view_partner_finance_revenue_aggregation
where data_month >= '2025-04-01'
and data_month < '2025-06-01'
and contract_id = 210;

select * from bi_dev.daron.view_partner_finance_revenue_aggregation
where contract_id = 211
and data_month = '2025-06-01';

drop schema bi_dev.daron;

select sum(gross_revenue) from BI.MANUAL_ENTRY_STAGE.sellside_manual_couponfollow_other_partner_revenue
where date_trunc('month', data_ts) = '2025-02-01';