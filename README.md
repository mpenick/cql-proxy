# cql-proxy (prototype)

A *client-side*  CQL proxy/sidecar. It listens on a local address and securely forwards your application's CQL traffic.

Run using `docker`:

Repo: https://hub.docker.com/repository/docker/mpenick/cql-proxy

```
docker run \
  --mount type=bind,source=$(pwd)/<your-secure-connect-zip>,target=/secure-connect-bundle.zip \
  -e TOKEN=<your-astra-token> \
  -p 127.0.0.1:9042:9042 mpenick/cql-proxy:latest
```

Or, run it locally (after building it):

Repo: https://github.com/mpenick/cql-proxy

```
cql-proxy --bundle <your-secure-connect-zip> --username token --password <your-astra-token>
```

Connect to your [DataStax Astra][astra] cluster using `127.0.0.1:9042`. `cqlsh` just works!

## Benefits

* Enables community drivers and legacy DataStax drivers to seemlessly work with [DataStax Astra][astra] without modification.
  * gocql
  * rust-driver
  * lua-cassandra
  * ...
* Existing C*/DSE applications can move to [DataStax Astra][astra] without any modification.

## Future ideas
  
* Allows for "connect", "query", "close" style drivers
* OSS posibilities: Automatic token-aware routing, cache prepared statements
  
## Status

* Very alpha, held together with duct tape and glue (it may crash!)
* Need to handle the `USE <keyspace>` problem. Session per keyspace?
* Many opportunites to harden and optimize (reduce allocations, granular locking, batched I/O)

[astra]: https://astra.datastax.com/
