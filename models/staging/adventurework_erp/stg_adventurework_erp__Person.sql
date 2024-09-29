with 

source as (

    select * from {{ source('adventurework_erp', 'Person') }}

),

person as (

    select
        cast(businessentityid as int) as pk_businessentityid
        ,cast(persontype as varchar) as persontype
        ,cast(namestyle as boolean) as namestyle
        ,cast(title as varchar) as title
        ,cast(firstname as varchar) as firstname
        ,cast(middlename as varchar) as middlename
        ,cast(lastname as varchar) as lastname
        ,cast(suffix as varchar) as suffix
        ,cast(emailpromotion as int) as emailpromotion
        ,cast(additionalcontactinfo as varchar) as additionalcontactinfo
        ,cast(demographics as varchar) as demographics

    from source

)

select * from person
