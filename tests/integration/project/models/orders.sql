select
    customers.id,
    {{ cents_to_dollars('payments.amount') }} as amount
from {{ ref('customers') }} as customers
inner join {{ source('raw', 'payments') }} as payments
    on customers.id = payments.customer_id;
