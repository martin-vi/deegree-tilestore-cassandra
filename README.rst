***************************
deegree-tilestore-cassandra
***************************

*caution: work in progress*

A deegree wmts tilestore implementation with cassandra as database backend.
Using the client library `datastax/java-driver`_ as client.

Why cassandra?
 * speed
 * high availability due to the use of p2p 
 * scaling
 * cloud integration

.. _datastax/java-driver: https://github.com/datastax/java-driver

deegree 3.3.9 `patch`_ to enable deegree-tilestore-cassandra.

.. _Patch: https://gist.github.com/anonymous/57b9cfef044ddcde3551

`tileCache2Cassandra.py`_ python script to populate cassandra with tiles from `TileCache`_ disk store

.. _tileCache2Cassandra.py: https://gist.github.com/martin-vi/dc174d3c45358387b4ee
.. _TileCache: http://tilecache.org/
