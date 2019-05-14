# flamingo
distributed cluster middleware system

# features
- Distributed job submission, job monitoring
- Job scheduling
- MatchMaking(starvation free, dynamic load balancing)
- job preemption and migration
- Fault tolerance(single fault) including general node failures and Master/Backup failure.
- Leader Election(Asynchronous multiple initiator) to elect a master
- Backup server(one backup for current master)
