with orders as (

    select * from {{ ref('stg_orders') }}

),

final as (

    select first_order,
           most_recent_order,
           CURRENT_DATE - first_order as customer_lifetime,
           number_of_orders
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
