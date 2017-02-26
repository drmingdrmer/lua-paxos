<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Concept](#concept)
  - [Proposer](#proposer)
  - [Acceptor](#acceptor)
  - [Round](#round)
  - [Value](#value)
  - [Value Round](#value-round)
  - [Version](#version)
  - [Quorum](#quorum)
  - [View](#view)
  - [Instance](#instance)
  - [Internal Request and Response](#internal-request-and-response)
    - [Request](#request)
    - [Response](#response)
    - [Response with error](#response-with-error)
  - [Phase-1](#phase-1)
    - [Phase-1 Request](#phase-1-request)
    - [Phase-1 Response](#phase-1-response)
  - [Phase-2](#phase-2)
    - [Phase-2 Request](#phase-2-request)
    - [Phase-2 Response](#phase-2-response)
  - [Phase-3](#phase-3)
    - [Phase-3 Request](#phase-3-request)
    - [Phase-3 Response](#phase-3-response)
- [Reference](#reference)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


## Concept

###   Proposer

Propose a new round of paxos, to establish a value.

The `value` established maybe not the `value` `proposer` specified, instead, it
could be the `value` that already exists( might be accepted by `quorum` ).

In this case, it is responsibility of the proposer to decide what to do: to
accept this value or start another round.

###   Acceptor

Receive and respond requests fro Proposer.

###   Round

Monotonically incremental value to identify a paxos round proposed by
proposer. Format:
```
(int, id_of_proposer)
```

###   Value

Any data proposed, accepted or committed.

###   Value Round

The `Round` in which value is established.

###   Version

`Version` is used to identify every committed `Value`.
Each time a `Value` is committed, `Version` increments by 1.

The initial `Version` is 0, which means nothing has been committed.

In this implementation, all of the data is stored altogether in one record:
```lua
{
    ver = 1,
    val = {
        view = {
            { a=1, b=1, c=1 },
            { b=1, c=1, d=1 },
        },
        leader = { ident="id", __expire=10000000 },
        action = {
            { name="bla", args={} },
            { name="foo", args={} },
        },
    }
}
```
It commits the entire record into storage as a single write operation.

###   Quorum

By definition it is subset of member that any two quorum must have non-empty
intersection.

In our implementation, in simple words, it is a sub set of more than half of
all members.

###   View

Membership of a paxos group.

View is no more than a common field in table `val`.
Proposer and Acceptor uses committed `view` as cluster that it belongs to.

Thus partially committed value would let Proposer and Acceptor sees different
cluster. But it does not break consensus. Following is how it is solved.

View is capable to be updated on the fly with the same failure tolerance as
normal paxos proposal.
Thus there is no down time during membership changing.

```lua
view = {
    { a=1, b=1, c=1 },
    { b=1, c=1, d=1 },
}
```
View consists of 1 or 2 sub table representing membership:
Being with only 1 sub table is the stable stat.

To change view from A to B, an intermediary stat of A + B is introduced to
connect two views that might be totally different.

Quorum(A)
Quorum(A)+Quorum(B)
Quorum(B)

At any time, committed view

Version of A is 10

1.  Commit dual view to quorum of cluster A.
1.  Now quorum becomes quorum(A) & quorum(B).
1.  Commit again to A + B. Make sure that majority of both A and B has view
    A+B.
1.  Commit B to A + B. Make sure that majority of both A and B has the view B
1.  Members in A but not in B find out that it is no more a member of the
    latest version of view. Then it destories itself.

Without intermedia A+B, there might be a gap in time none of A or B being able
to accept any value.


###   Instance

According to original paper [paxos][paxos_made_simple], paxos is
identified by `instance_id`.
In our implementation, there is only one record
thus `Instance` is identified just by `Version`.

On each `Acceptor`, there is only one legal `Instance` at any time:
the `Instance` for next version to commit.

Thus on `Acceptor`, if the version committed is 3, then only requests to
version 4 will be served. Other requests would be rejected with an error.

### Internal Request and Response

#### Request
```lua
{
    cmd="phase1",
    cluster_id="xx",
    ident="receiving_acceptor",
    ver="next_version",

    -- for phase1
    rnd={ int, proposer_id },

    -- for phase2
    rnd={ int, proposer_id },
    val={},

    -- for phase3
    val={},
}
```
*   cmd:

    phase1, phase2, phase3 or something else.

*   cluster_id:

    Identifier of this cluster.

*   ident:

    Identifier of Proposer sending this request.

*   ver:

    `Version`.

*   rnd:

    `Round`.

*   val:

    The `Value` to accept or to commit.

#### Response
Valid Response is always a table. Table content is specific to different
requests.


#### Response with error
```lua
{
    err = {
        Code = "string_code",
        Message = {},
    }
}
```
*   Code:

    String error name for program.

*   Message:

    Additional error information for human or further error handling.


###   Phase-1

AKA prepare.

####  Phase-1 Request
```lua
{
    cmd="phase1",
    cluster_id="xx",
    ident="receiving_acceptor",
    ver="next_version",

    rnd={ int, proposer_id },
}
```

####  Phase-1 Response
```lua
{
    rnd={ int, proposer_id },
    vrnd={ int, proposer_id },
    val={},
}
```
*   rnd:

    The latest `Round` acceptor ever seen, including the one in request.

*   vrnd:

    The `Round` in which `Value` was accepted.

    It is `nil` if no value accepted.

*   val:

    The `Value` accepted.

    It is `nil` if no value accepted.

###   Phase-2

AKA accept.

####  Phase-2 Request
```lua
{
    cmd="phase1",
    cluster_id="xx",
    ident="receiving_acceptor",
    ver="next_version",

    rnd={ int, proposer_id },
    val={}
}
```

####  Phase-2 Response
```lua
{}
```
If value was accepted, acceptor returns empty table.
Or an `err` field describing the error.

###   Phase-3

AKA commit.

####  Phase-3 Request
```lua
{
    cmd="phase1",
    cluster_id="xx",
    ident="receiving_acceptor",
    ver="next_version",

    val={}
}
```

####  Phase-3 Response
```lua
{}
```
If value was committed, acceptor returns empty table.
Or an `err` field describing the error.


## Reference

[paxos made simple][paxos_made_simple]

[paxos_made_simple]: http://www.google.com
