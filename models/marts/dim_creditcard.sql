-- models/marts/dimensions/dim_credit_card.sql

with
    credit_card as (
        select
            pk_creditcardid,
            cardtype,
            cardnumber,
            expmonth,
            expyear,
            modifieddate

        from {{ ref("stg_adventurework_erp__CreditCard") }}
    )

select
    pk_creditcardid as creditcard_id,
    cardtype,
    right(cardnumber, 4) as cardnumber_last4,
    expmonth,
    expyear,
    modifieddate

from credit_card

