with
    customer as (
        select
            pk_customerid
          , fk_personid
          , fk_storeid
          , fk_territoryid
        from {{ ref('stg_adventurework_erp__Customer') }}
    ),

    person as (
        select
            pk_businessentityid
          , firstname
          , middlename
          , lastname
          , emailpromotion
        from {{ ref('stg_adventurework_erp__Person') }}
    ),

    client_data as (
        select
            cust.pk_customerid
          , cust.fk_personid
          , cust.fk_storeid
          , cust.fk_territoryid
          , pers.firstname
          , pers.middlename
          , pers.lastname
          , pers.emailpromotion
        from customer cust
        left join person pers on cust.fk_personid = pers.pk_businessentityid
    ),

    -- Gerando a surrogate key para o cliente
    client_with_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['pk_customerid']) }} as customer_sk
          , cust_data.pk_customerid
          , cust_data.fk_storeid
          , cust_data.fk_territoryid
          , cust_data.firstname
          , cust_data.middlename
          , cust_data.lastname
          , cust_data.emailpromotion
        from client_data cust_data
    )

select
  customer_sk
  , pk_customerid
  , fk_storeid
  , fk_territoryid
  , firstname
  , middlename
  , lastname
  , emailpromotion
from client_with_sk
