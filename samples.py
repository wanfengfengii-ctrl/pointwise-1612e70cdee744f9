"""Small, actual warehouse correction workload used by the demo."""
INITIAL = {
    "batch_id":"open", "expected_generation":0, "changes":[
        {"source":"a","id":"s1","rev":1,"deleted":False,"k":"m","day":"2026-01-01","amount":100},
        {"source":"a","id":"s2","rev":1,"deleted":False,"k":None,"day":"2026-01-01","amount":7},
        {"source":"b","id":"b1","rev":1,"deleted":False,"k":"m","route":"r","region":"east"},
        {"source":"b","id":"b2","rev":1,"deleted":False,"k":"m","route":"r","region":"east"},
        {"source":"c","id":"c1","rev":1,"deleted":False,"route":"r","factor":2},
        {"source":"c","id":"c2","rev":1,"deleted":False,"route":"r","factor":3}
    ]}
CORRECTION = {
    "batch_id":"move", "expected_generation":1, "changes":[
        {"source":"b","id":"b1","rev":2,"deleted":False,"k":"m","route":"q","region":"west"},
        {"source":"b","id":"b2","rev":2,"deleted":True},
        {"source":"c","id":"c3","rev":1,"deleted":False,"route":"q","factor":-1}
    ]}
