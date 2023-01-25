from collections import OrderedDict
from typing import (
    Dict,
    List,
    Sequence,
    Set,
    cast,
)

from demeter.db import TableId

from .curses import FilterBy
from .data_option import DataOption
from .field_groups import FieldGroupSummary
from .log import getLogger

logger = getLogger()


def getFieldGroups(
    target: FieldGroupSummary,
    field_group_summaries: Dict[TableId, FieldGroupSummary],
) -> Sequence[TableId]:
    logger.warning("TARGET: %s", target)
    out: List[TableId] = []
    out.extend([f.parcel_id for f in target.fields])
    for i in target.field_group_ids:
        g = field_group_summaries[i]
        out.extend(getFieldGroups(g, field_group_summaries))
    logger.warning("LEN: %s", len(out))
    return out


def getFieldIds(filter_by: FilterBy) -> Set[TableId]:
    all_field_ids: Set[TableId] = set()
    if field_group_result := filter_by[DataOption.FIELD_GROUP]:
        selected = field_group_result.selected
        id_to_field_group = cast(
            OrderedDict[TableId, FieldGroupSummary], field_group_result.results
        )
        for s in selected:
            s = cast(FieldGroupSummary, s)
            field_ids = getFieldGroups(s, id_to_field_group)
            all_field_ids.update(field_ids)
    return all_field_ids
