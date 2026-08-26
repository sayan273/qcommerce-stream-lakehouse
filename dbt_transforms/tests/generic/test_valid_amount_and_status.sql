{% test assert_valid_order_metrics(model, column_name) %}

SELECT
    order_id,
    amount,
    status
FROM {{ model }}
WHERE 
    amount <= 0 
    OR status NOT IN ('SUCCESS', 'FAILED', 'PENDING')

{% endtest %}