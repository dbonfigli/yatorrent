# YATORRENT - yet another torrent client

A command line torrent client written in rust, implementing the [Torrent protocol v1.0](http://bittorrent.org/beps/bep_0003.html) ([detailed spec](https://wiki.theory.org/BitTorrentSpecification)) over TCP, with [Multitracker Metadata Extension](http://bittorrent.org/beps/bep_0012.html) and [UDP Tracker Protocol for BitTorrent](http://bittorrent.org/beps/bep_0015.html).

This is a didactic project I created purely to learn rust, it is far from feature complete or production ready, albeit working: it has been tested to saturate a 200Mb/s internet connection with low cpu usage.


Compile with:
```
$ cargo build --release
```
and run with:
```
$ yatorrent <torrent file location> <destination directory where to donwload files (optional, will use current dir if not provided)> <port where to listen for incoming connections (optional: default 8000)>
```
log levels are defined via the `RUST_LOG` environment variable (`trace`, `debug`, `info` (default), `warn`, `error`).


Things yet to be implemented / todos:
* optionally limit upload/download speed
* do not over-request blocks
* better choking algorithm
* better block requests pipelining algorithm: use also peer bandwith stats, tune request queue size
* check for stalled downloads / try new peers if no current one has a piece we want
* better endgame: ask the last remaining blocks to multiple peers to finish faster
* better algorithm to exlude bad peers for new connections
* remove not interested peers if we are also not interested
* avoid re-requesting blocks after receiving choke message for awhile - requested blocks could still come
* better command line args
* text-based UI / ncourses
* [DHT Protocol](http://bittorrent.org/beps/bep_0005.html)
* [uTorrent transport protocol](http://bittorrent.org/beps/bep_0029.html)
