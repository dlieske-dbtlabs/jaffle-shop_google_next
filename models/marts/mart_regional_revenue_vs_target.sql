{{
    config(
        materialized = 'table'
    )
}}

with

orders as (

    select
        location_id,
        date_trunc(cast(ordered_at as date), month)     as order_month,
        sum(order_total)                                as actual_revenue

    from {{ ref('orders') }}

    group by 1, 2

),

targets as (

    select
        location_id,
        target_month,
        monthly_revenue_target

    from {{ ref('stg_regional_targets') }}

),

joined as (

    select
        orders.location_id,
        orders.order_month                              as target_month,
        orders.actual_revenue,
        targets.monthly_revenue_target,

        round(
            case
                when targets.monthly_revenue_target is null
                    or targets.monthly_revenue_target = 0
                then null
                else (orders.actual_revenue / targets.monthly_revenue_target) * 100
            end,
            2
        )                                               as revenue_attainment_pct,

        round(
            orders.actual_revenue - coalesce(targets.monthly_revenue_target, 0),
            2
        )                                               as revenue_vs_target_delta

    from orders

    left join targets
        on  orders.location_id  = targets.location_id
        and orders.order_month  = targets.target_month

)

select * from joined
