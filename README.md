# YATORRENT - yet another torrent client

Yatorrent is a fast and lightweight BitTorrent client written from scratch in Rust, implementing the Torrent protocol v1.0 ([BEP 3](http://bittorrent.org/beps/bep_0003.html), [detailed spec](https://wiki.theory.org/BitTorrentSpecification)) over TCP, with the following extensions:

- [BEP 5 - DHT Protocol](http://bittorrent.org/beps/bep_0005.html);
- [BEP 6 - Fast Extension](https://www.bittorrent.org/beps/bep_0006.html);
- [BEP 9 - Extension for Peers to Send Metadata Files (Magnet links)](https://www.bittorrent.org/beps/bep_0009.html);
- [BEP 10 - Extension Protocol](http://bittorrent.org/beps/bep_0010.html);
- [BEP 11 - Peer Exchange (PEX)](https://www.bittorrent.org/beps/bep_0011.html);
- [BEP 12 - Multitracker Metadata Extension](http://bittorrent.org/beps/bep_0012.html);
- [BEP 15 - UDP Tracker Protocol for BitTorrent](http://bittorrent.org/beps/bep_0015.html);
- [BEP 23 - Tracker Returns Compact Peer Lists](https://www.bittorrent.org/beps/bep_0023.html).

## Supported Platforms

Yatorrent is cross-platform: runs on Linux, macOS, and Windows.

## Performance

Yatorrent has been tested to saturate a 1 Gb/s internet connection while maintaining low CPU usage. Memory at such speeds has always been under 50 MB/s. When used locally alongside other local clients, download speeds have instead been limited by disk I/O, reaching the maximum throughput the storage device can sustain, tested with up to 300MB/s.

## Quick Start

Compile with:

```
$ cargo build --release
```

and run with:

```
$ yatorrent -t <path to torrent file>
```

Examples:

```
$ ./target/release/yatorrent  -t ~/Downloads/ubuntu-26.04-desktop-amd64.iso.torrent -b ~/Downloads/ubuntu
```

or, use a magnet file (on this example, `ubuntu-26.04-desktop-amd64.iso`):

```
$ ./target/release/yatorrent -m "magnet:?xt=urn:btih:dafc8c076ca2f3ed376eeae7c76a0d6be2415c45" -b ~/Downloads/ubuntu
```

### Local Test

To test it both as a seeder and as a leecher locally, fully download the torrent, then block internet access, then on one shell:
```
$ ./target/release/yatorrent  -t ~/Downloads/ubuntu-26.04-desktop-amd64.iso.torrent -b ~/Downloads/ubuntu -s
```
and in another, the magnet link to also specify `x.pe` to directly know the other client:
```
$ ./target/release/yatorrent -m "magnet:?xt=urn:btih:dafc8c076ca2f3ed376eeae7c76a0d6be2415c45&x.pe=127.0.0.1:8000" -b ~/Downloads/ubuntu2 -s -p 8102 -d 8101
```
You can run as many clients as you want to test multiple local clients, as log as you select different ports with `-p` and `-d`, they will be able to know each other also via PEX.

### Usage

All command line arguments (show them with the `--help`), also definable via environment variables:

```
Usage: yatorrent [OPTIONS]

Options:
  -t, --torrent-file <TORRENT_FILE>
          Path to the .torrent file (optional, either this or a magnet link must be provided) [env: TORRENT_FILE=]
  -m, --magnet-uri <MAGNET_URI>
          Magnet Link URI (optional, either this or a torrent file must be provided) [env: MAGNET_URI=]
  -b, --base-path <BASE_PATH>
          Base path where files are downloaded (directory will be created if it does not exist) [env: BASE_PATH=] [default: _current working dir_]
  -p, --port <PORT>
          Listening port for Torrent protocol [env: PORT=] [default: 8000]
  -d, --dht-port <DHT_PORT>
          Listening port for DHT protocol [env: DHT_PORT=] [default: 8001]
  -l, --log-level <LOG_LEVEL>
          Log level [env: LOG_LEVEL=] [default: info] [possible values: trace, debug, info, warn, error]
  -s, --show-peers-stats
          Show detailed stats per peer [env: SHOW_PEERS_STATS=]
  -c, --max-connected-peers <MAX_CONNECTED_PEERS>
          Maximum number of connected peers allowed [env: MAX_CONNECTED_PEERS=] [default: 100]
  -z, --max-download-bandwidth <MAX_DOWNLOAD_BANDWIDTH>
          Max allowed total download bandwidth, with associated unit, e.g 10MiB (MiB is different from MB, the value is always bytes regardless of the case of "b", optional, no limit if not provided) [env: MAX_DOWNLOAD_BANDWIDTH=]
  -u, --max-upload-bandwidth <MAX_UPLOAD_BANDWIDTH>
          Max allowed total upload bandwidth, with associated unit, e.g 10MiB (MiB is different from MB, the value is always bytes regardless of the case of "b", optional, no limit if not provided) [env: MAX_UPLOAD_BANDWIDTH=]
  -e, --exit-when-complete
          Exit the client when the download is complete [env: EXIT_WHEN_COMPLETE=]
  -n, --no-dht
          Disable the DHT (Distributed Hash Table) to find peers without a central tracker (enabled by default) [env: NO_DHT=]
  -h, --help
          Print help
  -V, --version
          Print version
```

## TODOs

- check for stalled downloads / try new peers if no current one has a piece we want
- better algorithm to exclude bad peers for new connections
- remove not interested peers if we are also not interested if connection count is high
- text-based UI / ncourses
- [BEP 29 - uTorrent transport protocol](https://www.bittorrent.org/beps/bep_0029.html)
- [BEP 55 - Holepunch extension](https://www.bittorrent.org/beps/bep_0055.html)

## Why?

Yatorrent originally started as a didactic project to learn Rust, but it has since grown into a feature-reach BitTorrent client with substantial performance.
