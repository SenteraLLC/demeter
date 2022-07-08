from typing import Type, Any, Dict, Tuple, Optional, Any, Mapping
from typing import cast

import dictdiffer # type: ignore

from ... import constants
from ... import db
from ... import data
from ... import task

from ..datasource import DataSourceTypes


def makeDummyArguments(keyword_types : Dict[str, Type[Any]]) -> Dict[str, Any]:
  out : Dict[str, Any] = {}
  for name, typ in keyword_types.items():
    type_to_dummy : Mapping[Type[Any], Any] = {
      str : "",
      int : 0,
      float : 0.0,
    }
    out[name] = type_to_dummy.get(typ, None)
  return out


def keyword_type_to_enum(_type : Type[Any]) -> task.KeywordType:
  return {str : task.KeywordType.STRING,
          int : task.KeywordType.INTEGER,
          float : task.KeywordType.FLOAT
         }.get(_type, task.KeywordType.JSON)

def getSignature(cursor : Any,
                 function : task.Function,
                 input_types : DataSourceTypes,
                 keyword_types : Dict[str, Type[Any]],
                 output_types : Dict[str, Tuple[str, db.TableId]],
                ) -> task.FunctionSignature:
  def unpackS3Type(s3_type_and_subtype : Tuple[task.S3Type, Optional[task.TaggedS3SubType]]) -> task.S3TypeSignature:
    s3_type, maybe_tagged_s3_subtype = s3_type_and_subtype
    if maybe_tagged_s3_subtype is not None:
      tagged_s3_subtype = maybe_tagged_s3_subtype
      tag = tagged_s3_subtype.tag
      s3_subtype = tagged_s3_subtype.value
      if (tag == task.S3TypeDataFrame):
        s3_type_dataframe = cast(task.S3TypeDataFrame, s3_subtype)
        return s3_type, s3_type_dataframe
      else:
        raise Exception(f"Unhandled write for S3 sub-type: {tag} -> {s3_subtype}")
    return s3_type, None

  keyword_inputs = [task.Keyword(
                      keyword_name = keyword_name,
                      keyword_type = keyword_type_to_enum(keyword_type),
                    ) for keyword_name, keyword_type in keyword_types.items()
                   ]
  s3_inputs = [unpackS3Type(task.getS3Type(cursor, i)) for i in input_types["s3_type_ids"]]
  s3_outputs = [unpackS3Type(task.getS3Type(cursor, s3_type_id)) for (_, s3_type_id) in output_types.values()]

  return task.FunctionSignature(
           name = function.function_name,
           major = function.major,
           local_inputs = [data.getLocalType(cursor, i) for i in input_types["local_type_ids"]],
           keyword_inputs = keyword_inputs,
           http_inputs = [task.getHTTPType(cursor, i) for i in input_types["http_type_ids"]],
           s3_inputs = s3_inputs,
           s3_outputs = s3_outputs,
         )


def insertFunctionTypes(cursor : Any,
                        function : task.Function,
                        input_types : DataSourceTypes,
                        keyword_types : Dict[str, Type[Any]],
                        output_types : Dict[str, Tuple[str, db.TableId]],
                       ) -> Tuple[db.TableId, int]:
  function_id, minor = task.insertFunction(cursor, function)

  for i in input_types["local_type_ids"]:
    task.insertLocalParameter(cursor,
                              task.LocalParameter(
                                function_id = function_id,
                                local_type_id = i,
                             ))
  for i in input_types["s3_type_ids"]:
    task.insertS3InputParameter(cursor,
                                task.S3InputParameter(
                                function_id = function_id,
                                s3_type_id = i,
                                last_updated = constants.NOW,
                               ))
  for i in input_types["http_type_ids"]:
    task.insertHTTPParameter(cursor,
                             task.HTTPParameter(
                               function_id = function_id,
                               http_type_id = i,
                               last_updated = constants.NOW,
                             )
                            )
  for output_name, (s3_type_name, s3_type_id) in output_types.items():
    task.insertS3OutputParameter(cursor,
                                 task.S3OutputParameter(
                                   function_id = function_id,
                                   s3_output_parameter_name = output_name,
                                   s3_type_id = s3_type_id,
                                   last_updated = constants.NOW,
                                ))
  for keyword_name, keyword_type in keyword_types.items():
    task.insertKeywordParameter(cursor,
                                task.KeywordParameter(
                                  function_id = function_id,
                                  keyword_name = keyword_name,
                                  keyword_type = keyword_type_to_enum(keyword_type),
                                )
                               )
  return function_id, minor


def diffSignatures(cursor : Any,
                   candidate_signature : task.FunctionSignature,
                   maybe_latest_signature : Optional[task.FunctionSignature],
                   function : task.Function,
                  ) -> bool:
  if maybe_latest_signature is not None:
    latest_signature = maybe_latest_signature

    diff = list(dictdiffer.diff(candidate_signature, latest_signature))
    if len(diff) > 0:
      name = function.function_name
      major = function.major
      raise Exception(f"Cannot register function '{name}' with major {major}:\n Diff: {diff}")
  return True


def registerFunction(cursor : Any,
                     input_types : DataSourceTypes,
                     keyword_types : Dict[str, Type[Any]],
                     function : task.Function,
                     output_types : Dict[str, Tuple[str, db.TableId]],
                     maybe_latest_signature : Optional[task.FunctionSignature],
                    ) -> Optional[int]:
  function_id, new_minor = insertFunctionTypes(cursor, function, input_types, keyword_types, output_types)

  candidate_signature = getSignature(cursor, function, input_types, keyword_types, output_types)

  diffSignatures(cursor, candidate_signature, maybe_latest_signature, function)

  return new_minor

