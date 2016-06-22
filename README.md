# lua-poxos

Classic Paxos implementation in lua.

Nginx cluster management based on paxos.

Feature:

-   Classic two phase paxos algorithm.

-   Optional phase-3 as phase 'learn' or 'commit'

-   Support membership changing on the fly.

    This is archived by making the group members a paxos instance. Running paxos
    on group member updates group membership.

    Here we borrowed the concept 'view' that stands for a single version of
    membership.

    'view' is a no more than a normal paxos instance.


##  Getting Started
