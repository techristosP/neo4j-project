def readFile(file):
    with open(file, 'r') as f:
        data = f.readlines()
    return data


def parse_ACTIONS_FILE(data):
    userIDs = set()
    targetIDs = set()
    actionIDs = set()
    actions = []
    for i in range(len(data)):
        line = data[i].strip().split('\t')
        actionIDs.add(int(line[0]))
        userIDs.add(int(line[1]))
        targetIDs.add(int(line[2]))

        actions.append([int(line[0]), int(line[1]), int(line[2]), line[3]])
    
    return actionIDs, userIDs, targetIDs, actions

def parse_ACTION_LABELS_FILE(data, actions_attr):

    for i in range(len(data)):
        line = data[i].strip().split('\t')
        actions_attr[int(line[0])][0] = int(line[1])


def parse_ACTION_FEATURES_FILE(data, actions_attr):
    for i in range(len(data)):
        line = data[i].strip().split('\t')
        actions_attr[int(line[0])][1] = [x for x in line[1:]]


def write_NODE_CREATE_FILE(data, file, label):
    with open(file, 'w') as f:
        f.write('CREATE\n')

        template = "(:{label} {{id:{id}}}),\n"
        last_template = "(:{label} {{id:{id}}})"
        for line in data:
            # var = label.lower()
            id = line
            item = template if line != data[-1] else last_template
            item = item.format(id=id, label=label)
            f.write(item)

def write_RELATIONSHIP_CREATE_FILE(tuples, file, rlabel, attr):
    with open(file, 'w') as f:
        f.write('CREATE\n')

        template = "(user{user})-[:{rlabel} {{id:{id}, label:{label}, f0:{f0}, f1:{f1}, f2:{f2}, f3:{f3}, timestamp:{time}}}]->(target{target}),\n"
        last_template = "(user{user})-[:{rlabel} {{id:{id}, label:{label}, f0:{f0}, f1:{f1}, f2:{f2}, f3:{f3}, timestamp:{time}}}]->(target{target})"
        for action in tuples:
            id = action[0]
            user = action[1]
            target = action[2]
            time = action[3]
            action_label = attr[action[0]][0]
            action_features = attr[action[0]][1]
            item = template if action != tuples[-1] else last_template
            item = item.format(user=user, rlabel=rlabel,id=id, label=action_label, f0=action_features[0], f1=action_features[1], f2=action_features[2], f3=action_features[3], target=target, time=time)
            # item = f"(user{user})-[:Action {{id:{id}, label:{action_label}, features:[{action_features[0]}, {action_features[1]}, {action_features[2]}, {action_features[3]}], timestamp:{time}}}]->(target{target}),\n" if action != tuples[-1] else f"(user{user})-[:Action {{id:{id}, label:{action_label}, f_0:{action_features[0]}, f_1:{action_features[1]}, f_2:{action_features[2]}, f_3:{action_features[3]}, timestamp:{time}}}]->(target{target})"
            f.write(item)


def write_RELATIONSHIP_CSV_FILE(tuples, action_attr, file):
    with open(file, 'w') as f:
        f.write('action_id,label,f0,f1,f2,f3,user_id,target_id,timestamp\n')

        for t in tuples:
            id = t[0]
            user = t[1]
            target = t[2]
            timestamp = t[3]
            item = f"{id},{action_attr[id][0]},{action_attr[id][1][0]},{action_attr[id][1][1]},{action_attr[id][1][2]},{action_attr[id][1][3]},{user},{target},{timestamp}\n"
            f.write(item)


if __name__ == '__main__':
    data = readFile('act-mooc/mooc_actions.tsv')
    labels = readFile('act-mooc/mooc_action_labels.tsv')
    features = readFile('act-mooc/mooc_action_features.tsv')
    data.pop(0) # remove header
    labels.pop(0)
    features.pop(0)

    actions, users, targets, action_tuples = parse_ACTIONS_FILE(data)

    users = list(users)
    targets = list(targets)

    actions_attr = {action: [None, [None, None, None]] for action in actions}   # [label, [features]]
    action_labels = parse_ACTION_LABELS_FILE(labels, actions_attr)
    action_features = parse_ACTION_FEATURES_FILE(features, actions_attr)

    if all(item is not None for item in (actions, users, targets, action_tuples)):
        print("Data parsed successfully!")
    else:
        print("Some data is missing!")

    
    # write_NODE_CREATE_FILE(users, 'act-mooc/users.cypher', 'User')
    # write_NODE_CREATE_FILE(targets, 'act-mooc/targets.cypher', 'Target')
    # write_RELATIONSHIP_CREATE_FILE(action_tuples, 'act-mooc/actions.cypher', 'Action', actions_attr)
    # write_RELATIONSHIP_CSV_FILE(action_tuples, actions_attr, 'act-mooc/actions.csv')