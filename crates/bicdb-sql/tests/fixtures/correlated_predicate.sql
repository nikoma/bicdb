SELECT COUNT(*) FROM tasks WHERE  tasks.tenant_key=1 AND ( tasks.processed='N' AND tasks.state='OS' AND (

tasks.user_key=1

OR EXISTS (SELECT * FROM responsibles r WHERE tasks.responsible_key=r.responsible_key
AND r.kind='H' AND COALESCE(r.user_key,0)=0 AND COALESCE(r.role_key,0)=0 AND (tasks.user_key=1 OR tasks.user_key IS NULL))

OR EXISTS (SELECT * FROM responsibles r WHERE tasks.responsible_key=r.responsible_key
AND r.kind='H' AND r.user_key=1)

OR EXISTS (SELECT * FROM responsibles r INNER JOIN user_roles ur ON (r.role_key=ur.role_key)
WHERE tasks.responsible_key=r.responsible_key AND r.kind='R' AND ur.user_key=1 AND ur.enabled='Y')

OR EXISTS (SELECT * FROM approvers r WHERE tasks.task_key=r.task_key AND r.user_key=1 AND r.enabled='Y')
) AND tasks.tenant_key=1 )  AND tenant_key IN(0,1) AND (organization_key IN (0,1,2,3,4,5,6,7,8,9)) AND  (task_key IS NULL OR task_key NOT IN (SELECT record_key FROM private_access WHERE table_key=1 AND user_key<>1 AND enabled='Y'))
