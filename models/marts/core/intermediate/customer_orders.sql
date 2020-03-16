with orders as (

    select * from {{ ref('stg_orders') }}

),

final as (

    select *,
           CURRENT_DATE - first_order as customer_lifetime,
           CURRENT_DATE - most_recent_order as days_since_last_order
    FROM (
        select
            customer_id,
            min(order_date) as first_order,
            max(order_date) as most_recent_order,
            count(order_id) as number_of_orders
        from orders

        group by 1
    )
)

select * from final
