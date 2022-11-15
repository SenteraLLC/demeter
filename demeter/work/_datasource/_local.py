from typing import Any, Iterable, List, Tuple, Type, TypeVar, cast

from ... import data
from .._types import ExecutionSummary, ObservationArgument

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


def queryObservationByKey(
    cursor: Any,
    key: data.Key,
    local_type_id: int,
) -> Iterable[Tuple[data.ObservationValue, data.UnitType]]:
    stmt = """select V.*, U.unit, U.local_type_id
            from local_value V, unit_type U
            where V.unit_type_id = U.unit_type_id and U.local_type_id = %s and
              V.geom_id = %s and V.parcel_id = %s and (V.acquired > %s and V.acquired < %s)
          """

    geom_id = key.geom_id
    parcel_id = key.parcel_id
    start_date = key.start_date
    end_date = key.end_date

    args = (local_type_id, geom_id, parcel_id, start_date, end_date)

    cursor.execute(stmt, args)
    results = cursor.fetchall()

    local_value: List[data.ObservationValue] = sqlToTypedDict(results, data.ObservationValue)
    unit_type: List[data.UnitType] = sqlToTypedDict(results, data.UnitType)

    return zip(local_value, unit_type)


def loadType(
    cursor: Any,
    keys: List[data.Key],
    local_type_id: int,
) -> List[Tuple[data.ObservationValue, data.UnitType]]:
    results: List[Tuple[data.ObservationValue, data.UnitType]] = []
    for k in keys:
        partial_results = queryObservationByKey(cursor, k, local_type_id)
        results.extend(partial_results)
    return results


def loadObservationRaw(
    cursor: Any,
    keys: List[data.Key],
    observation_types: List[data.ObservationType],
    execution_summary: ExecutionSummary,
) -> List[Tuple[data.ObservationValue, data.UnitType]]:
    results: List[Tuple[data.ObservationValue, data.UnitType]] = []
    for observation_type in observation_types:
        maybe_observation_type_id = data.getMaybeObservationTypeId(cursor, observation_type)
        if maybe_observation_type_id is None:
            raise Exception(f"Failed to find ID for observation type: {observation_type}")
        observation_type_id = maybe_observation_type_id
        results_for_type = loadType(cursor, keys, observation_type_id)

        o = ObservationArgument(
            function_id=execution_summary.function_id,
            execution_id=execution_summary.execution_id,
            observation_type_id=observation_type_id,
            number_of_observations=len(results_for_type),
        )
        execution_summary.inputs["observation"].append(o)
        results.extend(results_for_type)
    return results


def getObservationRows(
    cursor: Any,
    keys: List[data.Key],
    observation_types: List[data.ObservationType],
    execution_summary: ExecutionSummary,
) -> List[Any]:
    rows: List[Any] = []
    for t in observation_types:
        maybe_observation_type_id = data.getMaybeObservationTypeId(cursor, t)
        if maybe_observation_type_id is None:
            raise Exception(f"data.Observation Type does not exist: {t}")
        else:
            observation_type_id = maybe_observation_type_id

    raw = loadObservationRaw(cursor, keys, observation_types, execution_summary)
    for observation_value, unit_type in raw:
        rows.append(dict(**observation_value, **unit_type))
    return rows
