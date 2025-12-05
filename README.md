# YATORRENT - yet another torrent client

A command line torrent client written in rust, implementing the [Torrent protocol v1.0](http://bittorrent.org/beps/bep_0003.html) ([detailed spec](https://wiki.theory.org/BitTorrentSpecification)) over TCP, with the following extensions:

- [Multitracker Metadata Extension](http://bittorrent.org/beps/bep_0012.html);
- [UDP Tracker Protocol for BitTorrent](http://bittorrent.org/beps/bep_0015.html);
- [DHT Protocol](http://bittorrent.org/beps/bep_0005.html);
- [Extension Protocol](http://bittorrent.org/beps/bep_0010.html);
- [Peer Exchange (PEX)](https://www.bittorrent.org/beps/bep_0011.html);
- [Extension for Peers to Send Metadata Files (Magnet links)](https://www.bittorrent.org/beps/bep_0009.html);
- [Tracker Returns Compact Peer Lists](https://www.bittorrent.org/beps/bep_0023.html).

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
  -t, --torrent-file <TORRENT_FILE>  Path to the .torrent file (optional, either this or a magnet link must be provided) [env: TORRENT_FILE=]
  -m, --magnet-uri <MAGNET_URI>      Magnet Link URI (optional, either this or a torrent file must be provided) [env: MAGNET_URI=]
  -b, --base-path <BASE_PATH>        Optional base path where files are downloaded (directory will be created if it does not exist) [env: BASE_PATH=] [default: /Users/diego/repo/yatorrent]
  -p, --port <PORT>                  Optional listening port [env: PORT=] [default: 8000]
  -d, --dht-port <DHT_PORT>          Optional listening port for DHT protocol [env: DHT_PORT=] [default: 8001]
  -l, --log-level <LOG_LEVEL>        Optional log level [env: LOG_LEVEL=] [default: info] [possible values: trace, debug, info, warn, error]
  -h, --help                         Print help
  -V, --version                      Print version
```

Things yet to be implemented / todos:

- optionally limit upload/download speed
- do not over-request blocks
- better choking algorithm
- better block requests pipelining algorithm: use also peer bandwidth stats, tune request queue size
- check for stalled downloads / try new peers if no current one has a piece we want
- better algorithm to exclude bad peers for new connections
- remove not interested peers if we are also not interested
- text-based UI / ncourses
