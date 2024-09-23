{% test delayed_period_not_negative(model, column_name) %}
    select
        *
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}
