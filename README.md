lua-paxos
=========

Classic paxos implementation in lua

Feature:

-   Classic two phase paxos algorithm.

-   Optional phase-3 as phase 'learn' or 'commit

-   Support changing group members on the fly.
    This is done by making the group members a paxos instance. Running paxos
    on group member updates group membership.
    Here we borrowed the concept 'view' that stands for a single version of
    membership.
    'view' is a no more than a normal paxos instance.



