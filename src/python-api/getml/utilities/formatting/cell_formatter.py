# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


import string


class CellFormatter(string.Formatter):
    """
     Custom formatter for cells in output columns. Supports all python-native format specs
     [https://docs.python.org/3/library/string.html#formatspec](https://docs.python.org/3/library/string.html#formatspec) plus the following custom
     format specs:

    - `<w_idth>.<p_recision>fd` (float): like `<w>.<p>f`, but the values of a column are
      decimal point aligned

     - `<w_idth>.<p_recision>d` (str): formats strings (holdings numbers) in a column on
       the decimal point by taking into account the precision
    """

    integer_overhang = 1

    # ------------------------------------------------------------

    def format_field(self, value, format_spec):
        if format_spec.endswith("fd"):
            return self._format_float_decimal_point(value, format_spec)

        if format_spec.endswith("d"):
            return self._format_string_decimal_point(value, format_spec)

        return super().format_field(value, format_spec)

    # ------------------------------------------------------------

    def _format_float_decimal_point(self, value, format_spec):
        width = int(format_spec.split(".")[0].strip())
        precision = int(format_spec.split(".")[1][0])
        padding = precision - self.integer_overhang

        if value.is_integer():
            formatted = f"{value:{width-padding}.{self.integer_overhang}f}"
            # fix misalignment due to missing decimal point
            if self.integer_overhang == 0:
                formatted = formatted[1:]
            return f"{formatted:{width}}"

        formatted = super().format_field(value, format_spec[:-1])
        stripped = formatted.rstrip("0")
        return f"{stripped:{width}}"

    # ------------------------------------------------------------

    def _format_string_decimal_point(self, value, format_spec):
        width = int(format_spec.split(".")[0].strip())
        precision = int(format_spec.split(".")[1][0])

        if value.strip("-").isdigit() or value == "nan":
            formatted = f"{value:>{width-precision-1}}"
            return f"{formatted:{width}}"

        if "." in value:
            digits = len(value.split(".")[1])
        else:
            digits = 0
        formatted = f"{value:>{width-precision+digits}}"
        return f"{formatted:{width}}"
