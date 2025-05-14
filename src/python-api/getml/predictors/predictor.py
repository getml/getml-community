# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Base class. Should not ever be directly initialized!
"""

import numbers
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar, Set

import numpy as np

from getml.feature_learning.validation import Validator
from getml.utilities.formatting import _SignatureFormatter

# --------------------------------------------------------------------


@dataclass(repr=False)
class _Predictor(ABC):
    """
    Base class. Should not ever be directly initialized!
    """

    _supported_params: ClassVar[Set]

    # ----------------------------------------------------------------

    def __post_init__(self):
        type(self)._supported_params = set(vars(self).keys())

        self.validate()

        for param in self._supported_params:
            setattr(type(self), param, Validator(param))

    # ----------------------------------------------------------------

    def __eq__(self, other):
        if not isinstance(other, _Predictor):
            raise TypeError("A predictor can only compared to another predictor!")

        # ------------------------------------------------------------

        # Check whether both objects have the same number of instance
        # variables.
        if len(set(self.__dict__.keys())) != len(set(other.__dict__.keys())):
            return False

        # ------------------------------------------------------------

        for kkey in self.__dict__:
            if kkey not in other.__dict__:
                return False

            # Take special care when comparing numbers.
            if isinstance(self.__dict__[kkey], numbers.Real):
                if not np.isclose(self.__dict__[kkey], other.__dict__[kkey]):
                    return False

            elif self.__dict__[kkey] != other.__dict__[kkey]:
                return False

        # ------------------------------------------------------------

        return True

    # ----------------------------------------------------------------

    def __repr__(self):
        return str(self)

    # ----------------------------------------------------------------

    def __setattr__(self, name, value):
        if name in self.__dataclass_fields__:  # pylint: disable=E1101
            super().__setattr__(name, value)
        else:
            raise AttributeError(
                f"Instance variable '{name}' is not supported in {self.type}."
            )

    # ----------------------------------------------------------------

    def __str__(self):
        sig = _SignatureFormatter(self)
        return sig._format()

    # ----------------------------------------------------------------

    def _getml_deserialize(self):
        # To ensure the getML can handle all keys, we have to add
        # a trailing underscore.
        encoding_dict = dict()

        for kkey in self.__dict__:
            encoding_dict[kkey + "_"] = self.__dict__[kkey]

        encoding_dict["type_"] = self.type

        return encoding_dict

    # ----------------------------------------------------------------

    @property
    def type(self):
        return type(self).__name__

    # ----------------------------------------------------------------

    @abstractmethod
    def validate(self, params=None):
        pass


# ------------------------------------------------------------------------------
