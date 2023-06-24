# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Reduces boilerplate code for the validation.
"""


def _validate(self, params):
    if params is None:
        params = self.__dict__
    else:
        params = {**self.__dict__, **params}

    if not isinstance(params, dict):
        raise ValueError("params must be None or a dictionary!")

    for kkey in params:
        if kkey not in self._supported_params:
            raise KeyError(
                f"Instance variable '{kkey}' is not supported in {self.type}."
            )

    return params
