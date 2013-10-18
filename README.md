# CARGO - Storage interface adapter

## Definitions

* peer - storage peer is host capable to support storage i/o
* cask - cask local reflection


## Supervisor tree

Application root supervisor manages i/o pools to storage peer.

```
 + cargo_sup : root supervisor
   |
   +-- cargo_peer_sup : connected storage peers
   |   |
   |   +-- pq (cargo_io_xxx) : i/o reader protocol pool
   |   |
   |   +-- pq (cargo_io_xxx) : i/o writer protocol pool 
  ...
```

Cargo casks are managed by owner application.

```
 + cargo_cask_sup
   |
   +-- cargo_cask : cask meta-data container 
   |
   +-- pq (cargo_cast_tx) : cast transaction pool
```

