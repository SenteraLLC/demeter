from typing import Any, Iterable, List, Tuple, Type, TypeVar, cast

from ... import data
from .._types import ExecutionSummary, LocalArgument

T = TypeVar("T")

# TODO: Does this get nested annotations?
def sqlToTypedDict(
    rows: List[Any],
    some_type: Type[Any],
) -> List[T]:
    return cast(
        List[T],
        [
            {
                k: v
                for k, v in row._asdict().items()
                if k in some_type.__annotations__.keys()
            }
            for row in rows
        ],
    )


def queryLocalByKey(
    cursor: Any,
    key: data.Key,
    local_type_id: int,
) -> Iterable[Tuple[data.LocalValue, data.UnitType]]:
    stmt = """select V.*, U.unit, U.local_type_id
            from local_value V, unit_type U
            where V.unit_type_id = U.unit_type_id and U.local_type_id = %s and
              V.geom_id = %s and V.field_id = %s and (V.acquired > %s and V.acquired < %s)
          """

    geom_id = key.geom_id
    field_id = key.field_id
    start_date = key.start_date
    end_date = key.end_date

    args = (local_type_id, geom_id, field_id, start_date, end_date)

    cursor.execute(stmt, args)
    results = cursor.fetchall()

    local_value: List[data.LocalValue] = sqlToTypedDict(results, data.LocalValue)
    unit_type: List[data.UnitType] = sqlToTypedDict(results, data.UnitType)

    return zip(local_value, unit_type)


def loadType(
    cursor: Any,
    keys: List[data.Key],
    local_type_id: int,
) -> List[Tuple[data.LocalValue, data.UnitType]]:
    results: List[Tuple[data.LocalValue, data.UnitType]] = []
    for k in keys:
        partial_results = queryLocalByKey(cursor, k, local_type_id)

        results.extend(partial_results)
    return results


def loadLocalRaw(
    cursor: Any,
    keys: List[data.Key],
    local_types: List[data.LocalType],
    execution_summary: ExecutionSummary,
) -> List[Tuple[data.LocalValue, data.UnitType]]:
    results: List[Tuple[data.LocalValue, data.UnitType]] = []
    for local_type in local_types:
        maybe_local_type_id = data.getMaybeLocalTypeId(cursor, local_type)
        if maybe_local_type_id is None:
            raise Exception(f"Failed to find ID for local type: {local_type}")
        local_type_id = maybe_local_type_id
        results_for_type = loadType(cursor, keys, local_type_id)

        l = LocalArgument(
            function_id=execution_summary.function_id,
            execution_id=execution_summary.execution_id,
            local_type_id=local_type_id,
            number_of_observations=len(results_for_type),
        )
        execution_summary.inputs["local"].append(l)

        results.extend(results_for_type)
    return results


def getLocalRows(
    cursor: Any,
    keys: List[data.Key],
    local_types: List[data.LocalType],
    execution_summary: ExecutionSummary,
) -> List[Any]:
    rows: List[Any] = []
    for t in local_types:
        maybe_local_type_id = data.getMaybeLocalTypeId(cursor, t)
        if maybe_local_type_id is None:
            raise Exception(f"data.Local Type does not exist: {t}")
        else:
            local_type_id = maybe_local_type_id

    raw = loadLocalRaw(cursor, keys, local_types, execution_summary)
    for local_value, unit_type in raw:
        rows.append(dict(**local_value, **unit_type))
    return rows
