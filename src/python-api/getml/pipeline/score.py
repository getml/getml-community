# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime

# --------------------------------------------------------------------


@dataclass
class Score(ABC):
    date_time: datetime
    set_used: str
    target: str

    @abstractmethod
    def __repr__(self):
        pass

    def __iter__(self):
        yield from asdict(self).items()


# --------------------------------------------------------------------


@dataclass
class ClassificationScore(Score):
    """
    Dataclass that holds data of a scoring run for a classification pipeline.

    Args:
        accuracy:
            The [`accuracy`][getml.pipeline.metrics.accuracy] of the classification.
        auc:
            The area under the curve: [`auc`][getml.pipeline.metrics.auc].
        cross_entropy:
            The [`cross_entropy`][getml.pipeline.metrics.cross_entropy].
    """

    accuracy: float
    auc: float
    cross_entropy: float

    def __repr__(self) -> str:
        return f"{self.date_time:%Y-%m-%d %H:%M:%S} {self.set_used} {self.target} {self.accuracy} {self.auc} {self.cross_entropy}"


# --------------------------------------------------------------------


@dataclass
class RegressionScore(Score):
    """
    Dataclass that holds data of a scoring run for a regression pipeline.

    Args:
        mae:
            The mean absolute error: [`mae`][getml.pipeline.metrics.mae]
        rmse:
            The root mean squared error: [`rmse`][getml.pipeline.metrics.rmse]
        rsquared:
            The squared correlation coefficient: [`rsquared`][getml.pipeline.metrics.rsquared]
    """

    mae: float
    rmse: float
    rsquared: float

    def __repr__(self) -> str:
        return f"{self.date_time:%Y-%m-%d %H:%M:%S} {self.set_used} {self.target} {self.mae} {self.rmse} {self.rsquared}"
