import pytest
import pandas as pd
import numpy as np

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../lambdas/mmm_dev_data_transfer')))
from lambda_function import detect_spend_mix_reallocation, _jensen_shannon_divergence

def test_jensen_shannon_divergence_identical():
    p = [0.5, 0.5]
    q = [0.5, 0.5]
    js = _jensen_shannon_divergence(p, q)
    assert js < 1e-6

def test_jensen_shannon_divergence_different():
    p = [1.0, 0.0]
    q = [0.0, 1.0]
    js = _jensen_shannon_divergence(p, q)
    assert js > 0.0

def test_detect_spend_mix_reallocation_no_drift():
    df = pd.DataFrame([{
        "tv": 500,
        "search": 500
    }])
    
    profile = {
        "mix_profile": {
            "spend_cols": ["tv", "search"],
            "avg_share": {
                "tv": 0.5,
                "search": 0.5
            }
        },
        "thresholds": {"MIX_SHIFT_YELLOW": 0.20, "JS_YELLOW": 0.06}
    }
    
    res = detect_spend_mix_reallocation(df, profile)
    assert res['detected'] is False

def test_detect_spend_mix_reallocation_max_delta_drift_yellow():
    # 75% tv, 25% search -> delta is 0.25 on both, which is >0.20 (yellow) but <0.30 (red)
    df = pd.DataFrame([{
        "tv": 750,
        "search": 250
    }])
    
    profile = {
        "mix_profile": {
            "spend_cols": ["tv", "search"],
            "avg_share": {
                "tv": 0.5,
                "search": 0.5
            }
        },
        "thresholds": {
            "MIX_SHIFT_YELLOW": 0.20, 
            "MIX_SHIFT_RED": 0.30,
            "JS_YELLOW": 0.06,
            "JS_RED": 0.10
        }
    }
    
    # We may trigger Yellow here due to max delta
    res = detect_spend_mix_reallocation(df, profile)
    assert res['detected'] is True
    assert res['severity'] == "YELLOW"

def test_detect_spend_mix_reallocation_drift_red():
    # 90% tv, 10% search -> delta is 0.40, > 0.30 (red)
    df = pd.DataFrame([{
        "tv": 900,
        "search": 100
    }])
    
    profile = {
        "mix_profile": {
            "spend_cols": ["tv", "search"],
            "avg_share": {
                "tv": 0.5,
                "search": 0.5
            }
        },
        "thresholds": {
            "MIX_SHIFT_YELLOW": 0.20, 
            "MIX_SHIFT_RED": 0.30,
            "JS_YELLOW": 0.06,
            "JS_RED": 0.10
        }
    }
    
    res = detect_spend_mix_reallocation(df, profile)
    assert res['detected'] is True
    assert res['severity'] == "RED"
