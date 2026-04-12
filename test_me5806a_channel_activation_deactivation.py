import pytest
import pandas as pd
import numpy as np

# We test the pure logic without importing lambda handlers as often they need mock AWS contexts
# Instead, we just copy the pure logic under test or assume it can be imported.
import sys
import os

# We will just import the function from lambda_function locally if it is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../lambdas/mmm_dev_data_transfer')))
from lambda_function import detect_channel_activation_deactivation

def test_detect_channel_activation_deactivation_no_drift():
    df = pd.DataFrame([{
        "tv": 1000,
        "search": 500,
        "social": 200
    }])
    
    profile = {
        "spend_stats": {
            "tv": {"count": 10, "active_weeks": 10},
            "search": {"count": 10, "active_weeks": 10},
            "social": {"count": 10, "active_weeks": 10}
        },
        "thresholds": {"MIN_ACTIVE_WEEKS_FOR_STABILITY": 8}
    }
    
    res = detect_channel_activation_deactivation(df, profile)
    assert res['detected'] is False
    assert res['severity'] == "NONE"


def test_detect_channel_activation_deactivation_activation_drift():
    df = pd.DataFrame([{
        "tv": 1000,
        "search": 500,
        "new_channel": 150 # just turned on
    }])
    
    profile = {
        "spend_stats": {
            "tv": {"count": 10, "active_weeks": 10},
            "search": {"count": 10, "active_weeks": 10},
            "new_channel": {"count": 3, "active_weeks": 3} # 3 weeks < 8 weeks threshold
        },
        "thresholds": {"MIN_ACTIVE_WEEKS_FOR_STABILITY": 8}
    }
    
    res = detect_channel_activation_deactivation(df, profile)
    assert res['detected'] is True
    assert res['severity'] == "RED"
    
    anomalies = res['anomalies']
    assert len(anomalies) == 1
    assert anomalies[0]['channel'] == 'new_channel'
    assert anomalies[0]['type'] == 'ACTIVATION'


def test_detect_channel_activation_deactivation_deactivation_drift():
    df = pd.DataFrame([{
        "tv": 0, # turned off!
        "search": 500
    }])
    
    profile = {
        "spend_stats": {
            "tv": {"count": 50, "active_weeks": 50}, # 50 >= 8 threshold
            "search": {"count": 50, "active_weeks": 50}
        },
        "thresholds": {"MIN_ACTIVE_WEEKS_FOR_STABILITY": 8}
    }
    
    res = detect_channel_activation_deactivation(df, profile)
    assert res['detected'] is True
    assert res['severity'] == "RED"
    
    anomalies = res['anomalies']
    assert len(anomalies) == 1
    assert anomalies[0]['channel'] == 'tv'
    assert anomalies[0]['type'] == 'DEACTIVATION'

def test_detect_channel_activation_deactivation_empty_df():
    df = pd.DataFrame([])
    profile = {}
    res = detect_channel_activation_deactivation(df, profile)
    assert res['detected'] is False
