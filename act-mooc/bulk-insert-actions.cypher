:auto load csv with headers from 'file:///actions.csv' as row
call {
    with row
    match (source:User {id: toInteger(row.user_id)})
    match (target:Target {id: toInteger(row.target_id)})
    merge (source)-[r:Action {id: toInteger(row.action_id), label: row.label, f0: toFloat(row.f0), f1: toFloat(row.f1), f2: toFloat(row.f2), f3: toFloat(row.f3), timestamp: row.timestamp}]->(target)
} in transactions of 2000 rows;