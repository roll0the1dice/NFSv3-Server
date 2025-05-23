## `mount` 操作分析 (via Portmapper)

This sequence shows the client trying to find the port for the NFS service.

---

### Portmap 报文解析
### Frame 18: Portmap GETPORT Call (Request for NFS Service Port)

*   **Frame Number:** 18
*   **Size:** 164 bytes on wire
*   **Interface:** `any`
*   **Capture Type:** Linux cooked capture

#### Layer 2: Linux cooked capture v1

#### Layer 3: Internet Protocol Version 4
*   **Source IP:** `127.0.0.1` (localhost)
*   **Destination IP:** `127.0.0.1` (localhost)

#### Layer 4: Transmission Control Protocol (TCP)
*   **Source Port:** `37832` (ephemeral port)
*   **Destination Port:** `111` (Portmapper/rpcbind)
*   **Seq:** 1, **Ack:** 1, **Len:** 96

#### Layer 7: Remote Procedure Call (RPC)
*   **Fragment header:** Last fragment, 92 bytes (RPC over TCP uses fragmentation)
*   **Transaction ID (XID):** `0x1ce435bc`
*   **Message Type:** `Call (0)`
*   **RPC Version:** `2`
*   **Program:** `Portmap (100000)`
*   **Program Version:** `2`
*   **Procedure:** `GETPORT (3)`
*   **Credentials:**
    *   **Flavor:** `AUTH_UNIX (1)`
    *   **Length:** 36
    *   **Stamp:** `0x00000000`
    *   **Machine Name:** `DESKTOP-AM4311C`
    *   **UID:** `0` (root)
    *   **GID:** `0` (root)
    *   **Auxiliary GIDs:** 0
*   **Verifier:**
    *   **Flavor:** `AUTH_NULL (0)`
    *   **Length:** 0
*   **Portmap GETPORT Call Details:**
    *   **Requested Program:** `NFS (100003)`
    *   **Requested Version:** `3`
    *   **Requested Protocol:** `TCP (6)` (implied by the `00 00 00 06` in the hex dump for protocol type)

**注意：通过 TCP 发送的 ONC RPC 报文**

*   TCP 是一个面向字节流的协议。它不保留消息边界。
*   当应用程序通过 TCP 发送数据时，TCP 只是将这些数据视为一个连续的字节流。接收方读取数据时，它也是一个字节流，不知道一个逻辑上的 "消息" 在哪里开始，哪里结束。
*   如果多个 RPC 消息被连续发送，或者一个大的 RPC 消息被分成多个 TCP 段发送，接收方的 RPC 层需要一种方法来：
    1.  **确定一个 RPC 消息的边界。**
    2.  **将可能被 TCP 分割的 RPC 消息片段重新组装起来。**
*   **Record Marking Standard (RMS) 就是为了解决这个问题而引入的。**
    *   每个 RPC 消息（或消息片段，如果 RPC 消息本身太大需要分段发送）在通过 TCP 发送前，都会在其前面加上一个 4 字节的 Record Marker。
    *   **Record Marker 格式：**
        *   **最高位 (MSB)：** Last Fragment Bit。
            *   `1`: 表示这是当前 RPC 消息的最后一个片段（或者消息本身就是一个片段）。
            *   `0`: 表示当前 RPC 消息后面还有更多片段。
        *   **其余 31 位：** Fragment Length。表示当前这个片段的长度（不包括 Record Marker 本身）。

    ```
    +---------------------+
    |   Ethernet Header   |
    +---------------------+
    |      IP Header      | (Protocol = 6 for TCP)
    +---------------------+
    |     TCP Header      |
    +---------------------+
    |  Record Marker (4B) |  <-- Present for ONC RPC over TCP
    |---------------------|
    | ONC RPC Message     |
    | (Fragment or Full)  |
    | (XDR Encoded)       |
    +---------------------+
    [Optional: More Record Markers + Fragments if needed]
    ```
    在下面的例子中，使用的是 TCP，所以存在 Record Marking (`80 00 00 5c`)。

#### Hex Dump (Frame 18)
```hex
0000  00 00 03 04 00 06 00 00 00 00 00 00 00 00 08 00   ................
0010  45 00 00 94 69 07 40 00 40 06 d3 5a 7f 00 00 01   E...i.@.@..Z....
0020  7f 00 00 01 93 c8 00 6f 08 72 1f 3b 2f 6a b4 2d   .......o.r.;/j.-
0030  80 18 02 00 fe 88 00 00 01 01 08 0a 47 4f a7 ed   ............GO..
0040  47 4f a7 ed 80 00 00 5c 1c e4 35 bc 00 00 00 00   GO.....\..5.....
0050  00 00 00 02 00 01 86 a0 00 00 00 02 00 00 00 03   ................
0060  00 00 00 01 00 00 00 24 00 00 00 00 00 00 00 0f   .......$........
0070  44 45 53 4b 54 4f 50 2d 41 4d 34 33 31 31 43 00   DESKTOP-AM4311C.
0080  00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00   ................
0090  00 00 00 00 00 01 86 a3 00 00 00 03 00 00 00 06   ................
00a0  00 00 00 00                                       ....
```
---

### Frame 20: Portmap GETPORT Reply (NFS Service Port)

*   **Frame Number:** 20
*   **Size:** 100 bytes on wire
*   **Interface:** `any`
*   **Capture Type:** Linux cooked capture

#### Layer 2: Linux cooked capture v1

#### Layer 3: Internet Protocol Version 4
*   **Source IP:** `127.0.0.1` (localhost, Portmapper)
*   **Destination IP:** `127.0.0.1` (localhost, client)

#### Layer 4: Transmission Control Protocol (TCP)
*   **Source Port:** `111` (Portmapper/rpcbind)
*   **Destination Port:** `37832` (ephemeral port, matching client's request)
*   **Seq:** 1, **Ack:** 97, **Len:** 32

#### Layer 7: Remote Procedure Call (RPC)
*   **Fragment header:** Last fragment, 28 bytes
*   **Transaction ID (XID):** `0x1ce435bc` (matches Frame 18)
*   **Message Type:** `Reply (1)`
*   **Reply State:** `accepted (0)`
*   **Verifier:**
    *   **Flavor:** `AUTH_NULL (0)`
    *   **Length:** 0
*   **Accept State:** `RPC executed successfully (0)`
*   **Portmap GETPORT Reply Details:**
    *   **Returned Port for NFS service (TCP):** `2049` (0x0801)

#### Hex Dump (Frame 20)
```hex
0000  00 00 03 04 00 06 00 00 00 00 00 00 00 00 08 00   ................
0010  45 00 00 54 d2 bd 40 00 40 06 69 e4 7f 00 00 01   E..T..@.@.i.....
0020  7f 00 00 01 00 6f 93 c8 2f 6a b4 2d 08 72 1f 9b   .....o../j.-.r..
0030  80 18 02 00 fe 48 00 00 01 01 08 0a 47 4f a7 ed   .....H......GO..
0040  47 4f a7 ed 80 00 00 1c 1c e4 35 bc 00 00 00 01   GO........5.....
0050  00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00   ................
0060  00 00 08 01                                       ....
```
**Summary for `mount`:** 客户端向（运行在 111 端口的）端口映射器（Portmapper）发送了一个 TCP 请求，目的是查询 NFS 服务（程序号 100003，版本 3，使用 TCP 协议）的端口号。这个请求使用了 `AUTH_UNIX` 凭据（一种认证方式）。端口映射器回复称，NFS 服务在 TCP 端口 `2049` 上可用（或监听）。因此，后续的 NFS RPC 调用（例如，用于挂载特定路径的调用）将会被导向（或发送到）这个端口。


### MOUNT 报文解析
#### Remote Procedure Call, Type:Call XID:0x208cc20c
*   **XID:** 0x208cc20c (546095628)
*   **Message Type:** Call (0)
*   **RPC Version:** 2
*   **Program:** MOUNT (100005)
*   **Program Version:** 3
*   **Procedure:** MNT (1)
*   [The reply to this request is in frame 838]
*   **Credentials**
    *    **Flavor:** AUTH_UNIX (1)
    *    **Length:** 40
    *    **Stamp:** 0x00000000
    *    **Machine Name:** DESKTOP-AM4311C
    *    **UID:** 0
    *    **GID:** 0
    *   **Auxiliary GIDs** (1) [0]
*   **Verifier**
    *    **Flavor:** AUTH_NULL (0)
    *    **Length:** 0
*   **Mount Service**
    *    [Program Version: 3]
    *    **[V3 Procedure:** MNT (1)]
    *    **Path:** /data/shared_folder

#### Hex Dump 
```hex
0000   00 00 00 00 00 00 00 00 00 00 00 00 08 00 45 00   ..............E.
0010   00 84 44 e4 40 00 40 11 4f 0f ac 1e a7 1c ac 1e   ..D.@.@.O.......
0020   a7 1c 03 b0 97 29 00 70 a6 f7 20 8c c2 0c 00 00   .....).p.. .....
0030   00 00 00 00 00 02 00 01 86 a5 00 00 00 03 00 00   ................
0040   00 01 00 00 00 01 00 00 00 28 00 00 00 00 00 00   .........(......
0050   00 0f 44 45 53 4b 54 4f 50 2d 41 4d 34 33 31 31   ..DESKTOP-AM4311
0060   43 00 00 00 00 00 00 00 00 00 00 00 00 01 00 00   C...............
0070   00 00 00 00 00 00 00 00 00 00 00 00 00 13 2f 64   ............../d
0080   61 74 61 2f 73 68 61 72 65 64 5f 66 6f 6c 64 65   ata/shared_folde
0090   72 00                                             r.
```




## `umount` 操作分析 (via Portmapper)

This sequence shows the client trying to find the port for the MOUNT service (typically used by `umount`).

---

### Frame 91: Portmap GETPORT Call (Request for MOUNT Service Port)

*   **Frame Number:** 91
*   **Size:** 100 bytes on wire
*   **Interface:** `any`
*   **Capture Type:** Linux cooked capture

#### Layer 2: Linux cooked capture v1

#### Layer 3: Internet Protocol Version 4
*   **Source IP:** `127.0.0.1` (localhost)
*   **Destination IP:** `127.0.0.1` (localhost)

#### Layer 4: User Datagram Protocol (UDP)
*   **Source Port:** `36788` (ephemeral port)
*   **Destination Port:** `111` (Portmapper/rpcbind)

#### Layer 7: Remote Procedure Call (RPC)
*   **Transaction ID (XID):** `0x6826f6d9`
*   **Message Type:** `Call (0)`
*   **RPC Version:** `2`
*   **Program:** `Portmap (100000)`
*   **Program Version:** `2`
*   **Procedure:** `GETPORT (3)`
*   **Credentials:**
    *   **Flavor:** `AUTH_NULL (0)`
    *   **Length:** 0
*   **Verifier:**
    *   **Flavor:** `AUTH_NULL (0)`
    *   **Length:** 0
*   **Portmap GETPORT Call Details:**
    *   **Requested Program:** `MOUNT (100005)`
    *   **Requested Version:** `3`
    *   **Requested Protocol:** `UDP (17)` (implied by the `00 00 00 11` in the hex dump for protocol type, though Wireshark text says UDP for the request itself)

#### Hex Dump (Frame 91)
```hex
0000  00 00 03 04 00 06 00 00 00 00 00 00 00 00 08 00   ................
0010  45 00 00 54 da 0f 40 00 40 11 62 87 7f 00 00 01   E..T..@.@.b.....
0020  7f 00 00 01 8f b4 00 6f 00 40 fe 53 68 26 f6 d9   .......o.@.Sh&..
0030  00 00 00 00 00 00 00 02 00 01 86 a0 00 00 00 02   ................
0040  00 00 00 03 00 00 00 00 00 00 00 00 00 00 00 00   ................
0050  00 00 00 00 00 01 86 a5 00 00 00 03 00 00 00 11   ................
0060  00 00 00 00                                       ....
```

### Frame 92: Portmap GETPORT Reply (MOUNT Service Port)

*   **Frame Number:** 92
*   **Size:** 72 bytes on wire
*   **Interface:** `any`
*   **Capture Type:** Linux cooked capture

#### Layer 2: Linux cooked capture v1

#### Layer 3: Internet Protocol Version 4
*   **Source IP:** `127.0.0.1` (localhost, Portmapper)
*   **Destination IP:** `127.0.0.1` (localhost, client)

#### Layer 4: User Datagram Protocol (UDP)
*   **Source Port:** `111` (Portmapper/rpcbind)
*   **Destination Port:** `36788` (ephemeral port, matching client's request)

#### Layer 7: Remote Procedure Call (RPC)
*   **Transaction ID (XID):** `0x6826f6d9` (matches Frame 91)
*   **Message Type:** `Reply (1)`
*   **Reply State:** `accepted (0)`
*   **Verifier:**
    *   **Flavor:** `AUTH_NULL (0)`
    *   **Length:** 0
*   **Accept State:** `RPC executed successfully (0)`
*   **Portmap GETPORT Reply Details:**
    *   **Returned Port for MOUNT service (UDP):** `40276` (0x9D54)

#### Hex Dump (Frame 92)
```hex
0000  00 00 03 04 00 06 00 00 00 00 00 00 00 00 08 00   ................
0010  45 00 00 38 53 cb 40 00 40 11 e8 e7 7f 00 00 01   E..8S.@.@.......
0020  7f 00 00 01 00 6f 8f b4 00 24 fe 37 68 26 f6 d9   .....o...$.7h&..
0030  00 00 00 01 00 00 00 00 00 00 00 00 00 00 00 00   ................
0040  00 00 00 00 00 00 9d 54                                       .......T
```
**Summary for `umount`:** 客户端向（运行在 111 端口的）端口映射器（Portmapper）发送了一个 UDP 请求，目的是查询 MOUNT 服务（程序号 100005，版本 3，使用 UDP 协议）的端口号。端口映射器回复称，MOUNT 服务在 UDP 端口 `40276` 上可用（或监听）。因此，后续的 `UMNT`（卸载）RPC 调用将会被导向（或发送到）这个端口。



00 00 00 1C  // File Handle Length: 28 bytes (0x1C)
01 00 07 00  // File Handle Data (start)
02 00 00 02
00 00 00 00
3E 3E 7D AE
34 C9 47 18  // File Handle Data (end - 28 bytes total)