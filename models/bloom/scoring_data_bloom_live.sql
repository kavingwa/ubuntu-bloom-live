{{ config(materialized='table') }}

select distinct * from (
WITH q3 AS (
         WITH q2 AS (
                 WITH trx_summary AS (
                         SELECT payments.store_number,
                            payments.transaction_time::date AS trx_date,
                            count(payments.id) AS daily_vol,
                            count(DISTINCT payments.phone) AS daily_customers_unq,
                            sum(payments.amount) AS daily_val,
                            cl.covid_limit,
                            cl.data_risk,
                            cl.prev_limit
                           FROM stg_mpesa_bloom.payments payments
                             JOIN asante_ods.dim_covid_limit cl ON payments.store_number = cl.accountnumber 
                             AND cl.data_risk in ('Low','Moderate')
                          WHERE payments.phone <> 'system'::text
                          GROUP BY payments.store_number, (payments.transaction_time::date), cl.covid_limit, cl.data_risk, cl.prev_limit
                        )
                 SELECT trx_summary.store_number,
                    trx_summary.covid_limit,
                    trx_summary.data_risk,
                    trx_summary.prev_limit,
                    sum(trx_summary.daily_val) AS total_trx_val,
                    round(avg(trx_summary.daily_customers_unq),0) AS avg_daily_customers,
                    round(avg(trx_summary.daily_vol),0) AS avg_daily_vol,
                    avg(trx_summary.daily_val) AS avg_daily_val,
                    min(trx_summary.trx_date) AS first_trx_date,
                    max(trx_summary.trx_date) AS last_trx_date,
                    1 + max(trx_summary.trx_date) - min(trx_summary.trx_date) AS expected_trx_days,
                    count(DISTINCT trx_summary.trx_date) AS actual_trx_days
                   FROM trx_summary
                  GROUP BY trx_summary.store_number, trx_summary.covid_limit, trx_summary.data_risk, trx_summary.prev_limit
                )
         SELECT q2.store_number,
            q2.covid_limit,
            q2.data_risk,
            q2.prev_limit,
            q2.total_trx_val,
            q2.avg_daily_customers,
            q2.avg_daily_vol,
            q2.avg_daily_val,
            q2.first_trx_date,
            q2.last_trx_date,
            q2.expected_trx_days,
            q2.actual_trx_days,
            30::numeric * q2.total_trx_val / q2.expected_trx_days::numeric AS approx_30_days_trx_val,
            q2.actual_trx_days::double precision / q2.expected_trx_days::double precision AS page_active_days,
                CASE
                    WHEN
                    CASE
                        WHEN q2.avg_daily_vol >= 5::numeric AND q2.avg_daily_customers > 4::numeric THEN true
                        ELSE false
                    END = true THEN 'Group 1 (5+)'::text
                    WHEN
                    CASE
                        WHEN q2.avg_daily_vol >= 4::numeric AND q2.avg_daily_customers > 3::numeric THEN true
                        ELSE false
                    END = true THEN 'Group 2(4)'::text
                    WHEN
                    CASE
                        WHEN q2.avg_daily_vol >= 3::numeric AND q2.avg_daily_customers > 2::numeric THEN true
                        ELSE false
                    END = true THEN 'Group 3(3)'::text
                    ELSE 'Group 4 (1-2)'::text
                END AS low_risk_segment,
                CASE
                    WHEN q2.avg_daily_vol >= 3::numeric THEN true
                    ELSE false
                END AS pass_avg_daily_vol,
                CASE
                    WHEN q2.avg_daily_customers >= 3::numeric THEN true
                    ELSE false
                END AS pass_avg_daily_customers,
                CASE
                    WHEN (q2.actual_trx_days::double precision / q2.expected_trx_days::double precision)::numeric > 0.7 THEN true
                    ELSE false
                END AS pass_days_active,
                CASE
                    WHEN q2.actual_trx_days >= 10 THEN true
                    ELSE false
                END AS pass_actual_trx_days,
                CASE
                    WHEN q2.last_trx_date > ('now'::text::date - 3) THEN 'Yes'
                    ELSE 'No'
                END AS transacted_last_5_days,
            0.3 * q2.total_trx_val AS pc30_monthly_paid_ins,
            GREATEST(LEAST(0.3 * (30::numeric * q2.total_trx_val / q2.expected_trx_days::numeric),
                CASE
                    WHEN q2.prev_limit = 0::numeric THEN 5000::numeric
                    ELSE q2.prev_limit
                END, 15000::numeric), 5000::numeric) AS proposed_limit
           FROM q2
        )
 SELECT DISTINCT lm.customeridnumber AS national_id_no,
    lm.accountnumber AS store_number,
    lm.merchant_name,
    q3.covid_limit,
    (case when q3.data_risk is null then 'High' else data_risk end) data_risk,
    q3.prev_limit,
    q3.total_trx_val,
    q3.avg_daily_customers,
    q3.avg_daily_vol,
    q3.avg_daily_val,
    q3.first_trx_date,
    q3.last_trx_date,
    q3.expected_trx_days,
    q3.actual_trx_days,
    q3.approx_30_days_trx_val,
    q3.page_active_days,
    q3.pass_avg_daily_vol,
    q3.pass_avg_daily_customers,
    q3.pass_days_active,
    q3.pass_actual_trx_days,
    q3.transacted_last_5_days,
    q3.low_risk_segment,
--    q3.proposed_limit,
    false = (false = q3.pass_avg_daily_vol OR false = q3.pass_avg_daily_customers OR false = q3.pass_avg_daily_vol OR false = q3.pass_days_active OR false = q3.pass_actual_trx_days OR 'No' = q3.transacted_last_5_days) AS approved_status,
        CASE
            WHEN lm.creditlimit > 0::numeric THEN false
            ELSE true
        END AS new_entry,
    lm.creditlimit AS current_limit,
    lm.datetimeadded AS limit_added_date,
    kyc.consent_for_survey,
    kyc.consent_for_info,
    kyc.repayment_period AS kyc_repayment_period,
    kyc.installment_type AS kyc_installment_type,
    kyc.loan_purpose AS kyc_loan_purpose,
    kyc.business_type AS kyc_business_type,
    kyc.number_of_businesses AS kyc_number_of_businesses,
    kyc.years_in_operation AS kyc_years_in_operation,
    kyc.supplier_expense AS kyc_supplier_expense,
    kyc.location_name AS kyc_location,
    kyc.age AS kyc_age,
    kyc.gender AS kyc_gender,
    flpu.loans_taken,
--    flpu.loans_rollover,
--    flpu.loans_default,
    flpu.loans_unpaid_due,
--    flpu.rollover_rate,
--    flpu.default_rate,
    flpu.has_loans_unpaid,
    flpu.loans_band,
    flpu.lifetime_in_months,
    flpu.loans_per_month,
    flpu.loans_taken_post_covid,
    flpu.current_limit as last_limit_from_mifos,
    last_limit_increase
   FROM q3
     LEFT JOIN asante_ods.dim_iprs_report lm ON q3.store_number = lm.accountnumber::text
     LEFT JOIN asante_ods.dim_bloom_kyc_info kyc ON kyc.account_number::text = lm.accountnumber::text
     LEFT JOIN apollo.fact_loans_summary_per_user flpu ON lm.customeridnumber::text = flpu.id_number::text
  WHERE lm.customeridnumber IS NOT null) as q