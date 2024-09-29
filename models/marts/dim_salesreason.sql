with
    salesreason as (
        select
            pk_salesreasonid
            ,salesreason_name
            ,reasontype

        from {{ ref("stg_adventurework_erp__SalesReason") }}
    )

select
    pk_salesreasonid
    ,salesreason_name
    ,reasontype

from joined
