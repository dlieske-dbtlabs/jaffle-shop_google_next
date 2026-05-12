with

source as (

    select * from {{ ref('regional_sales_targets') }}

),

renamed as (

    select

        ---------- ids
        cast(location_id as string)                         as location_id,

        ---------- dates
        cast(target_month as date)                          as target_month,

        ---------- numerics
        round(cast(monthly_revenue_target as numeric), 2)   as monthly_revenue_target,

        ---------- surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'location_id',
            'target_month'
        ]) }}                                               as regional_target_id

    from source

)

select * from renamed
