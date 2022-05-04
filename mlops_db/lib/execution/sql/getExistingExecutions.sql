with local_arguments as (
      select execution_id,
             coalesce(
               jsonb_agg(A),
               '[]'::jsonb
             ) as arguments
      from local_argument A
      join local_type T on T.local_type_id = A.local_type_id
      where A.function_id = %(function_id)s
      group by A.execution_id

    ), keyword_arguments as (
      select execution_id,
             coalesce(
               jsonb_agg(A),
               '[]'::jsonb
             ) as arguments
      from keyword_argument A
      join keyword_parameter P on P.keyword_name = A.keyword_name and P.function_id = A.function_id
      where A.function_id = %(function_id)s
      group by A.execution_id

    ), s3_arguments as (
      select execution_id,
             coalesce(
               jsonb_agg(A),
               '[]'::jsonb
             ) as arguments
      from s3_input_argument A
      join s3_type T on T.s3_type_id = A.s3_type_id
      where A.function_id = %(function_id)s
      group by A.execution_id

    ), http_arguments as (
      select execution_id,
             coalesce(
               jsonb_agg(A),
               '[]'::jsonb
             ) as arguments
      from http_argument A
      join http_type T on T.http_type_id = A.http_type_id
      where A.function_id = %(function_id)s
      group by A.execution_id

    ), s3_outputs as (
      select execution_id,
             coalesce(
               jsonb_agg(A),
               '[]'::jsonb
             ) as outputs
      from s3_output_argument A
      join s3_type T on T.s3_type_id = A.s3_type_id
      where A.function_id = %(function_id)s
      group by A.execution_id

    ), keys as (
       select EK.execution_id,
              E.function_id,
              jsonb_agg(
                jsonb_build_object('geospatial_key_id', GK.geospatial_key_id,
                                   'temporal_key_id', TK.temporal_key_id,
                                   'geom_id', GK.geom_id,
                                   'field_id', GK.field_id,
                                   'start_date', TK.start_date,
                                   'end_date', TK.end_date
                                  )
              ) as keys
      from execution_key EK
      join (select distinct execution_id from s3_outputs) D on D.execution_id = EK.execution_id
      join execution E on E.execution_id = EK.execution_id
      join geospatial_key GK on GK.geospatial_key_id = EK.geospatial_key_id
      join temporal_key TK on TK.temporal_key_id = EK.temporal_key_id
      group by EK.execution_id, E.function_id

    ) select K.execution_id,
             K.function_id,
             jsonb_build_object('local',   coalesce(L.arguments, '[]'::jsonb),
                                'keyword', coalesce(KW.arguments, '[]'::jsonb),
                                's3',      coalesce(S3I.arguments, '[]'::jsonb),
                                'http',    coalesce(H.arguments, '[]'::jsonb),
                                'keys',    K.keys
                               ) as inputs,
             jsonb_build_object('s3', S3O.outputs) as outputs
      from s3_outputs S3O
      left join local_arguments L on S3O.execution_id = L.execution_id
      left join keyword_arguments KW on S3O.execution_id = KW.execution_id
      left join http_arguments H on S3O.execution_id = H.execution_id
      left join s3_arguments S3I on S3O.execution_id = S3I.execution_id
      join keys K on S3O.execution_id = K.execution_id
      order by L.execution_id desc

