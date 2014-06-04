***************************
deegree-tilestore-cassandra
***************************

*caution: work in progress*

A deegree wmts tilestore implementation with cassandra as database backend.
Using the client library `Hector`_ as client.

Why cassandra?
 * speed
 * high availability due to the use of p2p 
 * scaling
 * cloud integration

.. _Hector: https://github.com/hector-client/hector

deegree 3.3.9 `patch`_ to enable deegree-tilestore-cassandra.

.. _Patch: https://gist.github.com/anonymous/57b9cfef044ddcde3551
