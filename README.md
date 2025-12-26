# YATORRENT - yet another torrent client

A command line torrent client written in rust, implementing the Torrent protocol v1.0 ([BEP 3](http://bittorrent.org/beps/bep_0003.html), [detailed spec](https://wiki.theory.org/BitTorrentSpecification)) over TCP, with the following extensions:

- [BEP 5 - DHT Protocol](http://bittorrent.org/beps/bep_0005.html);
- [BEP 6 - Fast Extension](https://www.bittorrent.org/beps/bep_0006.html);
- [BEP 9 - Extension for Peers to Send Metadata Files (Magnet links)](https://www.bittorrent.org/beps/bep_0009.html);
- [BEP 10 - Extension Protocol](http://bittorrent.org/beps/bep_0010.html);
- [BEP 11 - Peer Exchange (PEX)](https://www.bittorrent.org/beps/bep_0011.html);
- [BEP 12 - Multitracker Metadata Extension](http://bittorrent.org/beps/bep_0012.html);
- [BEP 15 - UDP Tracker Protocol for BitTorrent](http://bittorrent.org/beps/bep_0015.html);
- [BEP 23 - Tracker Returns Compact Peer Lists](https://www.bittorrent.org/beps/bep_0023.html).

This is a didactic project I created purely to learn rust, it is far from feature complete or production ready, albeit working: it has been tested to saturate a 1Gb/s internet connection with low cpu usage.

Compile with:

```
$ cargo build --release
```

and run with:

```
$ yatorrent -t <path to torrent file>
```

All command line arguments (show them with the `--help`), also definable via environment variables:

```
Usage: yatorrent [OPTIONS]

Options:
  -t, --torrent-file <TORRENT_FILE>
          Path to the .torrent file (optional, either this or a magnet link must be provided) [env: TORRENT_FILE=]
  -m, --magnet-uri <MAGNET_URI>
          Magnet Link URI (optional, either this or a torrent file must be provided) [env: MAGNET_URI=]
  -b, --base-path <BASE_PATH>
          Optional base path where files are downloaded (directory will be created if it does not exist) [env: BASE_PATH=] [default: _current working dir_]
  -p, --port <PORT>
          Optional listening port [env: PORT=] [default: 8000]
  -d, --dht-port <DHT_PORT>
          Optional listening port for DHT protocol [env: DHT_PORT=] [default: 8001]
  -l, --log-level <LOG_LEVEL>
          Optional log level [env: LOG_LEVEL=] [default: info] [possible values: trace, debug, info, warn, error]
  -s, --show-peers-stats
          Optional show peers stats, default false [env: SHOW_PEERS_STATS=]
  -c, --max-connected-peers <MAX_CONNECTED_PEERS>
          max connected peers [env: MAX_CONNECTED_PEERS=] [default: 100]
  -h, --help
          Print help
  -V, --version
          Print version
```

Things yet to be implemented / todos:

- cache writes
- optionally limit upload/download speed
- better choking algorithm
- check for stalled downloads / try new peers if no current one has a piece we want
- better algorithm to exclude bad peers for new connections
- remove not interested peers if we are also not interested if connection count is high
- text-based UI / ncourses
- [BEP 29 - uTorrent transport protocol](https://www.bittorrent.org/beps/bep_0029.html)
- [BEP 55 - Holepunch extension](https://www.bittorrent.org/beps/bep_0055.html)
