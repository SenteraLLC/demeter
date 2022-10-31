from typing import Generic

from ..summary import RawRowType
from .picker import Picker
from .table import Table


class SinglePicker(Picker[RawRowType]):
    def _do_select(self) -> bool:
        keep_going = super()._do_select()
        if keep_going:
            return False
        # TODO: Use 'Expected'
        return False

    def get_selected(self) -> RawRowType:
        return super().get_selected_rows()[0]
