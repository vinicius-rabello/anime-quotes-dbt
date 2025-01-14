with source_exercises as (
    select * from {{ source('gym_app_db', 'exercises') }}
    where muscle_group = 'Ombros'
), final as (
    select * from source_exercises
)

select * from final