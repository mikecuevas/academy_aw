with
    salesreason as (
        select
            pk_salesreasonid
            , salesreason_name
            , reasontype

        from {{ ref("stg_adventurework_erp__SalesReason") }}
    ),

    joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['pk_salesreasonid']) }} as salesreason_sk
            , pk_salesreasonid
            , salesreason_name
            , reasontype

        from salesreason
    )

select * from joined
