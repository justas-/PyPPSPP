# A native python client of Peer-to-Peer Streaming Peer Protocol (PPSPP) [RFC7574]

This repository contains a native Python3 client of Peer-to-Peer Streaming Peer Protocol and a bare-bones tracker server used to bootstrap the clients. At the moment, the client implements most of PPSPP standard and should be able to interoperate with [Libswift](https://github.com/libswift/libswift), the reference implementation of PPSPP protocol.

The goal is to keep the client in a stable condition in the **main** branch. Development (if any) is happening in **unstable** or feature branches.

### Usage

This PPSPP client can be used for files transfer or for multimedia streaming. The mode of client operation is determined based on the command-line parameters described below. Methods how to obtain a Merkle Tree Hash (Swarm ID) and how to run tracker server are explained in the following sections.

Sharing and downloading regular files:

```
python3 PyPPSPP.py
    # Required parameters
    --tracker <IP Address>          # IP Address of a tracker server
    --filename <Path>               # A path to a file that will be shared / downloaded
    --swarmid <Swarm ID string>     # An ID identifying clients swarm (Merkle Tree Root hash of a shared file)
    --filesize <Size in Bytes>      # Size of a shared file in bytes
    
    # Optional parameters
    --numpeers <Int>                # Limit a number of concurrent peers the client will connect to
    --identifier <String>           # Free text identifier added to the log/results file
    --tcp                           # Use TCP for connections between the peers (highly recommended for now)
    --workdir <Path>                # Change a current directory to the one indicated
```

A role of a client (seeder/leecher) will be determined based on a given file and a swarm ID. If the file is not empty and its Merkle Tree Root hash matches the given Swarm ID - the client will act as a seeder sharing the file. Otherwise (if a file is not found, or Merkle hash does not match the swarm ID) the file will be overwritten with an empty file and the client will start acting as a leecher.

Downloading Video-on-Demand file:

In the VoD use-case, the seeder is started as if it was sharing a regular file. The client is configured using the following command line parameters:

```
python3 PyPPSPP.py
    # Required parameters
    --tracker <IP Address>          # IP Address of a tracker server
    --swarmid <Swarm ID string>     # An ID identifying clients swarm (Merkle Tree Root hash of a shared file)
    --vod                           # This is VoD client
    
    # Optional parameters
    --skip <Int>                    # If a playback buffer is depleted, try jumping <Int> number of chunks forward. This allows the client to continue rendering a video stream if several missing chunks are blocking the rendering process
    --buffsz <Int>                  # The size of the download buffer in streaming content consumer
    --dlfwd <Int>                   # Try to download this many chunks ahead of the last chunk that was used to render a frame
```

Streaming and downloading a Live stream

Switching a client to the Live streaming mode is done by adding two additional command line parameters. See [ContentGenerator.py](https://github.com/justas-/PyPPSPP/blob/master/PyPPSPP/ContentGenerator.py) file to see how the live content is produced. 

**N.B.** Swarm ID has no meaning in a Live use-case. The only requirement is for all clients to use the same Swarm ID string.

```
python3 PyPPSPP.py
    # Use the same required parameters from the VoD use-case
    --live                          # This is a live streaming client
    --livesrc                       # This is a live streaming source
```


### Obtaining a Swarm ID / Merkle Tree Root hash

A FileUtil.py tool can be used to generate files with a given size having random content, and to generate Merkle Tree Root hash of a given (or generated) file. The usage is as follows:

```
python3 FileUtil.py
    --filename <Path>   # Filename that will be generated or used for hash calculation
    --create            # Create a file
    --size <Int>        # Size of a created file (in Bytes)
    --hash              # Calculate and print a hash of a given (or generated) file
```


### Running a Tracker Server

A tracker server is used to inform all new clients about other clients in the swarm. Tracker server is run as follows:

```
python3 TrackerServer.py
```

The server takes no command line parameters and binds itself to all local interfaces.

### Using Application-Layer Traffic Optimization (ALTO) to rank the peers

This PPSPP client can use ALTO server to rank the peers. A simple ALTO server can be found in my [PyALTO](https://github.com/justas-/PyALTO) project. In order to use ALTO, add the following optional command line parameters:

```
python3 PyPPSPP.py
    # Optional parameters enabling ALTO integration
    --alto                      # Use ALTO to rank the peers
    --altoserver <IP>           # IP Address of an ALTO server
    --altocosttype <String>     # Use the indicated ALTO cost-type
```

### Other information

Any bugs, ideas, suggestions and pull-requests should be made via GitHub. The source of the client is (C) Technical University of Denmark. All code is released to the public under the LGPL-3.0 license.
