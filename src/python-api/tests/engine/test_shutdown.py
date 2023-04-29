"""
This makes sure that the
engine shutdown works as intended.
"""

import getml


def test_shutdown():
    """
    This makes sure that the
    engine shutdown works as intended.
    """
    for _ in range(100):
        getml.engine.launch(in_memory=True)
        getml.engine.set_project("test_engine")
        getml.engine.shutdown()
