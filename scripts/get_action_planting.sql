
  with field_action_maybe_dupe as (
    select field_id,
           name,
           performed,
           row_number() over (partition by field_id, name
                              order by performed asc
                              RANGE BETWEEN INTERVAL '6 hours' PRECEDING AND INTERVAL '6 hours' FOLLOWING
                             ) as dupe_id
    from test_mlops.act A
    where performed > '2000-01-01' and
          field_id = any(%(field_ids)s::bigint[])

  ), field_actions as (
     select field_id,
            name,
            jsonb_agg(performed) as performed
    from field_action_maybe_dupe
    where dupe_id = 1
    group by field_id, name

  ), planting_maybe_dupe as (
    select P.field_id,
           C.species,
           C.cultivar,
           P.performed,
           P.details as planting_details,
           row_number() over (partition by P.field_id
                              order by P.performed asc
                              RANGE BETWEEN INTERVAL '6 hours' PRECEDING AND INTERVAL '6 hours' FOLLOWING
                             ) as dupe_id
    from test_mlops.planting P
    join test_mlops.crop_type C on P.crop_type_id = C.crop_type_id
    where P.field_id = any(%(field_ids)s::bigint[]) and P.performed > '2000-01-01'

  ), planting as (
    select field_id,
           species,
           cultivar,
           jsonb_agg(performed) as performed
    from planting_maybe_dupe
    where dupe_id = 1
    group by field_id, species, cultivar

  ) select F.field_id,
           jsonb_build_object(
             'actions', X.actions,
             'plantings', Y.plantings,
             'field_name', F.name,
             'field_external_id', F.external_id,
             'sentera_id', F.details->'AlmanacSenteraId',
             'farm_name', FARM.name,
             'region_name', REGION.name,
             'country_name', COUNTRY.name
           ) as field_meta
    from field F
    join field_group FARM on F.field_group_id = FARM.field_group_id
    join field_group REGION on FARM.parent_field_group_id = REGION.field_group_id
    join field_group COUNTRY on REGION.parent_field_group_id = COUNTRY.field_group_id
    left join (
      select field_id,
             jsonb_agg(
               jsonb_build_object(
                 'name', name,
                 'performed', performed
               )
             ) as actions
      from field_actions
      where name is not null and performed is not null
      group by field_id
    ) X on F.field_id = X.field_id
    left join (
      select field_id,
             jsonb_agg(
               jsonb_build_object(
                 'species', species,
                 'cultivar', cultivar,
                 'performed', performed
               )
             ) as plantings
      from planting
      where species is not null and performed is not null
      group by field_id
    ) Y on X.field_id = Y.field_id