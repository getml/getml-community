# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Reduces boilerplate code for the validation.
"""


def _validate(instance, params):
    if params is None:
        params = instance.__dict__
    else:
        params = {**instance.__dict__, **params}

    if not isinstance(params, dict):
        raise ValueError("params must be None or a dictionary!")

    for kkey in params:
        if kkey not in instance._supported_params:
            raise KeyError(
                f"Instance variable '{kkey}' is not supported in {instance.type}."
            )

    return params
