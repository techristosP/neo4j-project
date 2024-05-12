from graphdb import Neo4j
from loadData import *

neo = Neo4j(password="moocmooc")
# Set to True to create relationships from python driver, False to write to csv file
# CAUTION: Setting to True will be very slow!
createRelationshipsFromPython = False

def createNodes(IDs, label):
    """
    Create nodes in the graph database with the given IDs and label.

    Args:
        IDs (list): A list of IDs for the nodes.
        label (str): The label to assign to the nodes.

    Returns:
        None
    """
    IDs = list(IDs)

    query = "CREATE\n"
    for id in IDs:
        query += f"(:{label} {{id:{id}}}),\n"
    query = query[:-2]
    result = neo.queryDB(query)

    print(f"{label} nodes created!")

def create_node_index(label, property):
    """
    Creates an index for a specific property on nodes with a given label.

    Args:
        label (str): The label of the nodes to create the index for.
        property (str): The property to create the index on.

    Returns:
        None

    Raises:
        None
    """
    query = f"create index for (x:{label}) on (x.{property})"
    result = neo.queryDB(query)

    print(f"Index created on {label} nodes!")

def createRelationship(tx, data, action_attr):
    """
    Creates a relationship between a User and a Target node in the graph database.

    Args:
        tx: The transaction object used to execute the Cypher query.
        data: A list of actions containing the action ID, user ID, target ID, timestamp, and other attributes.
        action_attr: A dictionary mapping action IDs to action labels and features.

    Returns:
        None
    """
    for action in data:
        id = action[0]
        user = action[1]
        target = action[2]
        time = action[3]
        action_label = action_attr[id][0]
        action_features = action_attr[id][1]

        if action_label is None: action_label = 'None'

        tx.run("MATCH (u:User {id: toInteger($user_id)}) "
               "MATCH (t:Target {id: toInteger($target_id)}) "
               "MERGE (u)-[a:Action {id: toInteger($id), label: toString($label), f0: toFloat($f0), f1: toFloat($f1), f2: toFloat($f2), f3: toFloat($f3), timestamp: toString($time)}]->(t)",
               user_id=user, target_id=target, id=id, label=action_label, f0=action_features[0], f1=action_features[1], f2=action_features[2], f3=action_features[3], time=time)


if __name__ == '__main__':
    data = readFile('act-mooc/mooc_actions.tsv')
    data.pop(0) # remove header

    actions, users, targets, action_tuples = parse_ACTIONS_FILE(data)

    # Create User and Target nodes
    createNodes(users, 'User')
    createNodes(targets, 'Target')

    # Create index on User and Target nodes
    create_node_index('User', 'id')
    create_node_index('Target', 'id')


    labels = readFile('act-mooc/mooc_action_labels.tsv')
    features = readFile('act-mooc/mooc_action_features.tsv')
    labels.pop(0)
    features.pop(0)

    actions_attr = {action: [None, [None, None, None]] for action in actions}   # [label, [features]]
    action_labels = parse_ACTION_LABELS_FILE(labels, actions_attr)
    action_features = parse_ACTION_FEATURES_FILE(features, actions_attr)

    """
    Used cypher's load_csv function to load the data from the csv file created below 
    because python driver was very slow for large datasets.
    The script in bulk-insert-actions.cypher needs ms to insert the whole dataset.
    """
    # Create Action relationships
    if createRelationshipsFromPython:
        print("\nCreating Action relationships...")
        neo.batch_queryDB(createRelationship, action_tuples, actions_attr, 4000)
        print("\nAction relationships created!")
    else:
        write_RELATIONSHIP_CSV_FILE(action_tuples, actions_attr, 'act-mooc/actions.csv')
        print("\nAction relationships written to csv file!")

    
    neo.close()