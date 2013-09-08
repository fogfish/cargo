# Storage interface adapter


## Supervisor tree

Application root supervisor manages i/o pools to storage peer.
Individual buckets managed by applications.

```
 + cargo_sup : root supervisor
   |
   +-- cargo_peer_sup : connected storage peers
   |   |
  ...  +-- pq (i/o reader)
       |
       +-- pq (i/o writer)

```