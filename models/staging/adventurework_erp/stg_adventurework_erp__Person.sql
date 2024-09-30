with 

source as (

    select * from {{ source('adventurework_erp', 'Person') }}

),

person as (
    select
        cast(businessentityid as int) as pk_businessentityid
        , cast(firstname as varchar) as firstname
        , cast(middlename as varchar) as middlename
        , cast(lastname as varchar) as lastname
        , cast(emailpromotion as int) as emailpromotion
    from source
)

select * from person
