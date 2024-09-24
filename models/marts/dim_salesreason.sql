-- models/marts/dimensions/dim_salesreason.sql

with
    salesreason as (
        select
            pk_salesreasonid,
            salesreason_name,
            reasontype,
            modifieddate

        from {{ ref("stg_adventurework_erp__SalesReason") }}
    ),

    joined as (
        select
            pk_salesreasonid,
            salesreason_name,
            reasontype,
            modifieddate

        from salesreason
    )

select
    pk_salesreasonid as salesreason_id,
    salesreason_name,
    reasontype,
    modifieddate

from joined
