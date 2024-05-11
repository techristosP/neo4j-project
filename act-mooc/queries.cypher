//Queries:

//2. Count all users, count all targets, count all actions
match (u:User) return count(u) as numOfUsers;
match (t:Target) return count(t) as numOfTargets;
match ()-[a:Action]->() return count(a) as numOfActions;

//3. Show all actions (actionID) and targets (targetID) of a specific user (choose one)
match (u:User)-[a:Action]->(t:Target) where u.id=5000 return u, a, t;

//4. For each user, count his/her actions
match (u:User)-[a:Action]->(:Target) return u.id as userID, count(a) as numOfActions;

//5. For each target, count how many users have done this target
match (u:User)-[:Action]->(t:Target) return t.id as targetID, count(distinct u) as numOfUsers;

//6. Count the average actions per user
match (u:User)-[a:Action]->(:Target)
with u, count(a) as userActions
return avg(userActions) as averageUserActions;

//7. Show the userID and the targetID, if the action has positive Feature2
match (u:User)-[a:Action]->(t:Target) where a.f2>0 return u, a, t;

//8. For each targetID, count the actions with label “1”
match (u:User)-[a:Action]->(t:Target) where a.label='1' return t.id as targetID, count(u.id) as numOfLabel_1_actions;

//check above: match (t:Target {id:20})<-[a:Action]-(u:User) where a.label='1' return t,u;