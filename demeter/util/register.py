from typing import Type, Any, Dict, Tuple, Optional, Any, Mapping
from typing import cast

import dictdiffer # type: ignore

from ..types.inputs import KeywordType, S3TypeDataFrame, TaggedS3SubType, S3Type, Keyword
from ..inputs import getS3Type, getHTTPType, getLocalType
from ..types.function import S3InputParameter, HTTPParameter, S3OutputParameter
from ..function import insertLocalParameter, insertS3InputParameter, insertHTTPParameter, insertS3OutputParameter, insertKeywordParameter, insertFunction

from ..database.types_protocols import TableId
from ..datasource.types import DataSourceTypes
from ..datasource.register import DataSourceRegister

from ..types.function import Function, FunctionSignature, S3TypeSignature, LocalParameter, KeywordParameter
from ..constants import NOW


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


def keyword_type_to_enum(_type : Type[Any]) -> KeywordType:
  return {str : KeywordType.STRING,
          int : KeywordType.INTEGER,
          float : KeywordType.FLOAT
         }.get(_type, KeywordType.JSON)

def getSignature(cursor : Any,
                 function : Function,
                 input_types : DataSourceTypes,
                 keyword_types : Dict[str, Type[Any]],
                 output_types : Dict[str, Tuple[str, TableId]],
                ) -> FunctionSignature:
  def unpackS3Type(s3_type_and_subtype : Tuple[S3Type, Optional[TaggedS3SubType]]) -> S3TypeSignature:
    s3_type, maybe_tagged_s3_subtype = s3_type_and_subtype
    if maybe_tagged_s3_subtype is not None:
      tagged_s3_subtype = maybe_tagged_s3_subtype
      tag = tagged_s3_subtype.tag
      s3_subtype = tagged_s3_subtype.value
      if (tag == S3TypeDataFrame):
        s3_type_dataframe = cast(S3TypeDataFrame, s3_subtype)
        return s3_type, s3_type_dataframe
      else:
        raise Exception(f"Unhandled write for S3 sub-type: {tag} -> {s3_subtype}")
    return s3_type, None

  keyword_inputs = [Keyword(
                      keyword_name = keyword_name,
                      keyword_type = keyword_type_to_enum(keyword_type),
                    ) for keyword_name, keyword_type in keyword_types.items()
                   ]
  s3_inputs = [unpackS3Type(getS3Type(cursor, i)) for i in input_types["s3_type_ids"]]
  s3_outputs = [unpackS3Type(getS3Type(cursor, s3_type_id)) for (_, s3_type_id) in output_types.values()]

  return FunctionSignature(
           name = function.function_name,
           major = function.major,
           local_inputs = [getLocalType(cursor, i) for i in input_types["local_type_ids"]],
           keyword_inputs = keyword_inputs,
           http_inputs = [getHTTPType(cursor, i) for i in input_types["http_type_ids"]],
           s3_inputs = s3_inputs,
           s3_outputs = s3_outputs,
         )


def insertFunctionTypes(cursor : Any,
                        function : Function,
                        input_types : DataSourceTypes,
                        keyword_types : Dict[str, Type[Any]],
                        output_types : Dict[str, Tuple[str, TableId]],
                       ) -> Tuple[TableId, int]:
  function_id, minor = insertFunction(cursor, function)

  for i in input_types["local_type_ids"]:
    insertLocalParameter(cursor,
                         LocalParameter(
                           function_id = function_id,
                           local_type_id = i,
                         ))
  for i in input_types["s3_type_ids"]:
    insertS3InputParameter(cursor,
                           S3InputParameter(
                             function_id = function_id,
                             s3_type_id = i,
                             last_updated = NOW,
                             details = None,
                           ))
  for i in input_types["http_type_ids"]:
    insertHTTPParameter(cursor,
                                   HTTPParameter(
                                     function_id = function_id,
                                     http_type_id = i,
                                     last_updated = NOW,
                                     details = None,
                                   ))
  for output_name, (s3_type_name, s3_type_id) in output_types.items():
    insertS3OutputParameter(cursor,
                                       S3OutputParameter(
                                         function_id = function_id,
                                         s3_output_parameter_name = output_name,
                                         s3_type_id = s3_type_id,
                                         last_updated = NOW,
                                         details = None,
                                       ))

  for keyword_name, keyword_type in keyword_types.items():
    insertKeywordParameter(cursor,
                                      KeywordParameter(
                                        function_id = function_id,
                                        keyword_name = keyword_name,
                                        keyword_type = keyword_type_to_enum(keyword_type),
                                      )
                                     )

  return function_id, minor


def diffSignatures(cursor : Any,
                   candidate_signature : FunctionSignature,
                   maybe_latest_signature : Optional[FunctionSignature],
                   function : Function,
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
                     function : Function,
                     output_types : Dict[str, Tuple[str, TableId]],
                     maybe_latest_signature : Optional[FunctionSignature],
                    ) -> Optional[int]:
  function_id, new_minor = insertFunctionTypes(cursor, function, input_types, keyword_types, output_types)

  candidate_signature = getSignature(cursor, function, input_types, keyword_types, output_types)

  diffSignatures(cursor, candidate_signature, maybe_latest_signature, function)

  return new_minor


