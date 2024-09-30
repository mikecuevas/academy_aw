with
    credit_card as (
        select
            pk_creditcardid
          , cardtype
        from {{ ref('stg_adventurework_erp__CreditCard') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['pk_creditcardid']) }} as creditcard_sk
    , pk_creditcardid
    , cardtype
from credit_card
