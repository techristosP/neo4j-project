from graphdb import Neo4j

neo = Neo4j(password="moocmooc")

def count_users():
    query = """
            match (u:User) return count(u) as numOfUsers;
            """
    result = neo.queryDB(query).pop()
    print(f"Number of users: {result['numOfUsers']}")

def count_targets():
    query = """
            match (t:Target) return count(t) as numOfTargets;
            """
    result = neo.queryDB(query).pop()
    print(f"Number of targets: {result['numOfTargets']}")

def count_actions():
    query = """
            match ()-[a:Action]->() return count(a) as numOfActions;
            """
    result = neo.queryDB(query).pop()
    print(f"Number of actions: {result['numOfActions']}")

def show_actions_targets_byUser(id):
    query = f"match (u:User)-[a:Action]->(t:Target) where u.id={id} return u, a, t"
    result = neo.queryDB(query)
    # result.sort(key=lambda x: x['t']['id'])
    print(type(result))
    print(result)
    # for r in result:
    #     print(f"User: {r['u']['id']}, Action: {r['a']}, Target: {r['t']['id']}")

def count_user_actions():
    query = """
            match (u:User)-[a:Action]->(:Target) return u.id as userID, count(a) as numOfActions
            limit 100
            """
    result = neo.queryDB(query)
    print("Number of actions for each user: ")
    for rec in result:
        print(f"- userID {rec['userID']}: {rec['numOfActions']}")

def count_target_users():
    query = """
            match (u:User)-[:Action]->(t:Target) return t.id as targetID, count(distinct u) as numOfUsers
            """
    result = neo.queryDB(query)
    print("Number of users for each target: ")
    for rec in result:
        print(f"- targetID {rec['targetID']}: {rec['numOfUsers']}")

def average_actions_per_user():
    query = """
            match (u:User)-[a:Action]->(:Target)
            with u, count(a) as userActions
            return avg(userActions) as averageUserActions
            """
    result = neo.queryDB(query).pop()
    print(f"Average number of actions per user: {result['averageUserActions']}")

def show_positive_f2_users_targets():
    query = """
            match (u:User)-[a:Action]->(t:Target) where a.f2>0 return u, t
            limit 100
            """
    result = neo.queryDB(query)
    print("Users and Targets with positive f2: ")
    for rec in result:
        print(f"- userID: {rec['u']['id']}, targetID: {rec['t']['id']}")


def count_label_1_actions():
    query = """
            match (u:User)-[a:Action]->(t:Target) where a.label='1' return t.id as targetID, count(u.id) as numOfLabel_1_actions
            """
    result = neo.queryDB(query)
    print("Number of label 1 actions for each target: ")
    for rec in result:
        print(f"- targetID {rec['targetID']}: {rec['numOfLabel_1_actions']}")


if __name__ == "__main__":

    count_users()
    count_targets()
    count_actions()

    # TODO
    # show_actions_targets_byUser(5000)

    count_user_actions()
    count_target_users()
    average_actions_per_user()
    show_positive_f2_users_targets()
    count_label_1_actions()


    neo.close()