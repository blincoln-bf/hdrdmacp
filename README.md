# hdrdmacp : RDMA client/server for file transfers

## Introduction

The hdrdmacp utlity is a small program that can be used to copy large files between
two computers connected via IB (infiniband) using RDMA (Remote Direct Memory Access).
The program was written to transfer large data sets in the form of many 20GB files
as part of the Data Acquisition for a Nulear Physics experiment at Jefferson Lab (http://www.jlab.org).
To use it, just run one instance in server mode on the machine you want the file transferred TO:

> hdrdmacp -s

And run it in client mode on the machine you wish to transfer the file FROM, giving
it the local file as the first argument and then the remote host+destination filename
as the second:

> hdrdmacp file.dat my.server.host:/path/to/dest/filename

notes:

1. This only supports copying from the client node to the server node at the moment.
It would be fairly straightforward to enhance it to allow copies in the other direction
as well. Let me know if you would like to have that functionality.

2. This currently requires the destination be a filename. Thus, one cannot give just the 
directory on the destination. Relative filenames will be relative to the directory the
server was started in.  Use the "-P" option to create the destination directory if it
doesn't already exist.

3. This was written to run on some memory-heavy machines so the default buffer sizes
are quite large. The may be changed with some command-line options (see below).

## Prerequisites

### Libraries

Building a working version of hdrdmacp requires the following libraries and their associated headers:

* `libpthread`
* `libverbs` or `libibverbs`
* `zeromq`
* `zlib`

e.g. to build hdrdmacp on Debian 12, the following command will install the necessary packages:

```
sudo apt install rdma-core libibverbs1 librdmacm1 libibmad5 libibumad3 librdmacm1 ibverbs-providers rdmacm-utils infiniband-diags libfabric1 ibverbs-utils libzmq3-dev libibverbs-dev libz-dev
```

on Fedora 41:

```
sudo dnf install rdma-core libibverbs libibverbs-devel libibverbs-utils zeromq zeromq-devel zlib-devel
```

### Sufficient max locked memory limit

Some Unix and Linux distributions default to a low value for the maximum locked memory limit, and this will cause hdrdmacp to exit with the following error:

```
ERROR: Unable to register memory region! errno=12
       (Please see usage statement for a possible work around)
```

Before running hdrdmacp, run the `ulimit -l` command. If the output is small, such as Fedora's default of 8192, you will need to increase the limit. Debian's default of 2558652 seems to be sufficient for at least basic use, but you can also use the special `unlimited` value to remove the limit entirely.

Non-`root` users are likely unable to increase their own limit, so consider one of the following options:

1 - `su` to `root`, then run `ulimit -l unlimited` before running hdrdmacp.
2 - As `root`, add the following two lines to /etc/security/limits.conf, then log off and back on:

```
@wheel          hard    memlock 2558652
@wheel          soft    memlock 2558652
```

## Building

Commands for downloading and building are below. There is a SConscript file which can be
used if you have scons installed. Since the source all gets compile into a single 
program though, it is also easy to just build it via a single command as shown.

> git clone https://github.com/JeffersonLab/hdrdmacp

> cd hdrdmacp

> c++ -DHAVE_ZEROMQ=1 --std=c++11 -g -o hdrdmacp *.cc -libverbs -lz -lpthread -lzmq -I.

## Running

Run the program with "--help" to get the help statement:

<pre>
Hall-D RDMA file copy server/client

Usage:

   hdrdmacp [options] srcfile host:[port:]destfile
   hdrdmacp -s

This program can be used as both the server and client to copy a
file from the local host to a remote host using RDMA over IB.
This currently does not support copying files from the remote
server back to the client. It also only supports copying a single
file per connection at the moment. In server mode it can accept
multiple simultaneous connections and so can receive any number
of files. In client mode however, only a single file can be
tranferred. Run multiple clients to transfer multiple files.

Note: In the options below: 
    CMO=Client Mode Only
    SMO=Server Mode Only

 options:
    -c         calculate checksum (adler32 currently only prints) (CMO)
    -d         delete source file upon successful transfer (CMO)
    -g  group  set effective group (useful if run as a system service) (SMO)
    -h         print this usage statement.
    -m  GB     total memory to allocate (def. 8GB for server, 1GB for client)
    -n  Nbuffs number of buffers to break the allocated memory into. This
               will determine the size of RDMA transfer requests.
    -P         make parent directory path on remote host if needed (CMO)
    -p  port   set remote port to connect to (can also be given in dest name) (CMO)
    -s         server mode (SMO)
    -sp port   server port to listen on (default is 10470) (SMO)
    -u  user   set effective user (useful if run as a system service) (SMO)
    -v         increase verbosity level
    -q         quiet mode (set verbosity level to 0)
    -zp port   port to publish stats as zeroMQ messages to
    -cp port   port to listen for zeroMQ control messages on
    -cmd host "command arg ..." send command to specified host

 (run with --help for extended help)
</pre>

## Example

### Start the server on the destination system

```
$ ./hdrdmacp -m 1 -n 4 -s
Looking for IB devices ...

=============================================
Found 1 devices
---------------------------------------------
   device 0 : ibp2s0 : uverbs0 : IB : InfiniBand channel adapter : Num. ports=2 : port num=1 : lid=3
=============================================

Device ibp2s0 opened. num_comp_vectors=16
Port attributes:
           state: 4
         max_mtu: 5
      active_mtu: 5
  port_cap_flags: 39405672
      max_msg_sz: 1073741824
    active_width: 2
    active_speed: 4
      phys_state: 5
      link_layer: 1
Created 4 buffers of 250MB (1GB total)
Launching hdRDMAstats thread ...
Launching hdRDMAcontrol thread ...
hdRDMAstats::Publish called
hdRDMAcontrol::Publish called
Listening for connections on port ... 10470
```

### Transfer a file from the source system

```
$ ./hdrdmacp /home/user/test_file ibtest2:/home/user/test_file
Looking for IB devices ...

=============================================
Found 1 devices
---------------------------------------------
   device 0 : ibp3s0 : uverbs0 : IB : InfiniBand channel adapter : Num. ports=2 : port num=1 : lid=2
=============================================

Device ibp3s0 opened. num_comp_vectors=16
Port attributes:
           state: 4
         max_mtu: 5
      active_mtu: 5
  port_cap_flags: 38881384
      max_msg_sz: 1073741824
    active_width: 2
    active_speed: 4
      phys_state: 5
      link_layer: 1
Created 4 buffers of 250MB (1GB total)
IP address: 172.16.253.11 (ibtest2)
Connected to ibtest2:10470
Sending file: /home/user/test_file-> (ibtest2:)/home/user/test_file   (0.000292292 GB)
  queued 0MB (0/0 MB -- 100%  - 30.7032 Gbps)   
  Transferred 0.292292 MB in 0.000919991 sec  (2541.69 Mbps)
  I/O rate reading from file: 7.176e-05 sec  (32585.5 Mbps)
  Confirmed remote file size matches local: 292292 bytes
```
