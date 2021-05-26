with payments as (

    select * from {{ ref('stg_payments') }}

),

orders as (
    
    select * from {{ ref('stg_orders') }}

),

order_payments as (
    
    select
        order_id
        ,sum(case when status = 'success' then amount end) as amount
    from payments

    group by order_id

)

select
    orders.order_id
    ,orders.customer_id
    ,orders.order_date
    ,orders.status
    ,order_payments.amount
from orders

left join order_payments
on orders.order_id = order_payments.order_id
