"""
Module test_processor.py
"""

from .. import processor

def test_join_dfs():
    """ Test join_dfs function with valid parameters """
    processor.join_dfs('', '', '')
    assert True

def test_run():
    """ Test run function with valid parameters """
    processor.run('', '', '')
    assert True
