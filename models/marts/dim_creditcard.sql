with
    credit_card as (
        select
            pk_creditcardid
            ,cardtype
            ,modifieddate

        from {{ ref("stg_adventurework_erp__CreditCard") }}
    )

select
    pk_creditcardid
    ,cardtype
    ,modifieddate

from credit_card

