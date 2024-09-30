with 

source as (

    select * from {{ source('adventurework_erp', 'Address') }}

),

address as (

    select
        cast(addressid as int) as pk_addressid
        , cast(stateprovinceid as int) as fk_stateprovinceid
        , cast(addressline1 as varchar) as addressline1
        , cast(city as varchar) as city
        , cast(postalcode as varchar) as postalcode
        , cast(spatiallocation as varchar) as spatiallocation

    from source

)

select * from address
