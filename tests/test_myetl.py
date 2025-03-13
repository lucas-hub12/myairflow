# myetl test
from myairflow.myetl_function import load_data_pq, save_agg_csv

def test_myetl():
    msg = "pytest:LUCAS"
    l = load_data(msg)
    t = save_agg_csv(msg)
    assert l == True
    assert t == True
