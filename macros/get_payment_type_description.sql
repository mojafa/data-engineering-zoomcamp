-- Macros in Jinja are pieces of code that can be reused multiple times
-- they are analogous to "functions" in other programming languages, and are
-- extremely useful if you find yourself repeating code across multiple models. 
-- Macros are defined in .sql files, typically in your macros directory

 {#
    This macro returns the description of the payment_type 
#}

{% macro get_payment_type_description(payment_type) -%}

    case {{ payment_type }}
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end

{%- endmacro %}

