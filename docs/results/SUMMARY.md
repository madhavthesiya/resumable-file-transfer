# Results

Generated from `bench_runs.csv` by `bench/summarize.py`; every number below is the mean over the repeats recorded there.

## Integrity

- runs recorded: **340**
- runs reaching `status=ok`: **340**
- files verified by CRC32: **2147270**
- CRC mismatches: **0**
- bytes delivered: **28.9 GB**

Every recorded run completed and every received file matched its CRC32.

> Throughput figures are **loopback** measurements on one host: they are bounded by memory and scheduler behaviour, not by a network link, so compare them against each other and not against a link rate.

## E1 - baseline: threading mode vs concurrent clients

Topic `cane`, no faults. Makespan is wall-clock until the last client finishes; goodput is total useful bytes over that makespan.

| clients | single: makespan s | multi: makespan s | single: Mbit/s | multi: Mbit/s |
|---:|---:|---:|---:|---:|
| 1 | 0.73 &plusmn;0.22 | 0.33 &plusmn;0.05 | 735 &plusmn;253 | 1521 &plusmn;226 |
| 2 | 0.81 &plusmn;0.26 | 0.40 &plusmn;0.08 | 1346 &plusmn;462 | 2532 &plusmn;391 |
| 4 | 2.43 &plusmn;0.44 | 1.04 &plusmn;1.18 | 823 &plusmn;133 | 3087 &plusmn;1298 |
| 8 | 4.01 &plusmn;1.12 | 1.45 &plusmn;0.59 | 1056 &plusmn;317 | 3361 &plusmn;1796 |

## E2 - baseline: goodput vs topic size

| topic | files | MB | makespan s | Mbit/s |
|---|---:|---:|---:|---:|
| scoiattolo | 1862 | 24.2 | 0.18 &plusmn;0.06 | 1202 &plusmn;306 |
| cane | 4863 | 57.9 | 0.57 &plusmn;0.15 | 923 &plusmn;279 |
| gatto | 1668 | 144.1 | 0.91 &plusmn;0.72 | 1771 &plusmn;661 |

## E3 - restart vs resume across the fault rate

restart has no checkpoint, so a fault restarts the whole topic; resume continues from the last checkpoint. `redundant` is bytes that had to be sent more than once.

**restart** (topic `scoiattolo`)

| p | expected faults/pass | makespan s | Mbit/s | redundant MB | x topic size | reconnects |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 0.00 | 0.15 &plusmn;0.04 | 1404 &plusmn;314 | 0.0 &plusmn;0.0 | 0.00 | 0.0 |
| 0.0002 | 0.31 | 0.28 &plusmn;0.23 | 1106 &plusmn;543 | 6.3 &plusmn;10.6 | 0.26 | 0.6 |
| 0.0005 | 0.78 | 1.50 &plusmn;1.81 | 579 &plusmn;509 | 34.8 &plusmn;38.0 | 1.44 | 3.1 |
| 0.001 | 1.55 | 6.73 &plusmn;5.26 | 93 &plusmn;120 | 92.1 &plusmn;59.8 | 3.80 | 12.9 |
| 0.002 | 3.10 | 52.01 &plusmn;43.92 | 16 &plusmn;31 | 471.8 &plusmn;363.3 | 19.47 | 96.0 |

**resume** (topic `scoiattolo`)

| p | expected faults/pass | makespan s | Mbit/s | redundant MB | x topic size | reconnects |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 0.00 | 0.86 &plusmn;0.22 | 260 &plusmn;95 | 0.0 &plusmn;0.0 | 0.00 | 0.0 |
| 0.0002 | 0.31 | 1.21 &plusmn;0.69 | 230 &plusmn;118 | 0.0 &plusmn;0.0 | 0.00 | 0.5 |
| 0.0005 | 0.78 | 1.05 &plusmn;0.66 | 236 &plusmn;75 | 0.0 &plusmn;0.0 | 0.00 | 1.7 |
| 0.001 | 1.55 | 3.03 &plusmn;1.04 | 76 &plusmn;27 | 0.0 &plusmn;0.0 | 0.00 | 5.4 |
| 0.002 | 3.10 | 3.33 &plusmn;3.10 | 98 &plusmn;55 | 0.0 &plusmn;0.0 | 0.00 | 8.0 |

## E4 - restart vs resume on a large-file topic

The same comparison where single files are large, which is where restart-from-scratch costs the most.

**restart** (topic `gatto`)

| p | expected faults/pass | makespan s | Mbit/s | redundant MB | x topic size | reconnects |
|---:|---:|---:|---:|---:|---:|---:|
| 0.0001 | 0.92 | 1.00 &plusmn;0.80 | 2208 &plusmn;1455 | 156.0 &plusmn;156.2 | 1.08 | 2.8 |

**resume** (topic `gatto`)

| p | expected faults/pass | makespan s | Mbit/s | redundant MB | x topic size | reconnects |
|---:|---:|---:|---:|---:|---:|---:|
| 0.0001 | 0.92 | 1.18 &plusmn;0.93 | 1287 &plusmn;350 | 0.0 &plusmn;0.0 | 0.00 | 1.5 |

## E5 - parallel: K parallel connections, fault-free

One client, K TCP connections to the same server thread, which multiplexes them with epoll.

| K | makespan s | Mbit/s | speedup | reconnects | redundant MB |
|---:|---:|---:|---:|---:|---:|
| 1 | 3.65 &plusmn;2.25 | 196 &plusmn;109 | 1.00x | 0.0 | 0.0 |
| 2 | 2.63 &plusmn;1.10 | 225 &plusmn;103 | 1.15x | 0.0 | 0.0 |
| 4 | 1.39 &plusmn;0.34 | 366 &plusmn;65 | 1.87x | 0.0 | 0.0 |
| 8 | 1.68 &plusmn;1.01 | 337 &plusmn;83 | 1.72x | 0.0 | 0.0 |

## E6 - parallel under faults

Same topic and p, comparing a single connection against K=4.

| K | makespan s | Mbit/s | speedup | reconnects | redundant MB |
|---:|---:|---:|---:|---:|---:|
| 1 | 3.26 &plusmn;0.98 | 161 &plusmn;40 | 1.00x | 5.7 | 0.0 |
| 4 | 1.49 &plusmn;0.20 | 331 &plusmn;44 | 2.06x | 5.7 | 0.0 |

## E7 - fault-free comparison on a single topic

Every implementation on topic `scoiattolo`, one client, p=0. The other experiments each use whichever topic suits what they measure, so their p=0 rows sit on different workloads and cannot be divided by one another; this experiment exists to make the comparison like-for-like.

| implementation | makespan s | Mbit/s | vs baseline | overhead % |
|---|---:|---:|---:|---:|
| baseline (multi-threaded) | 0.09 &plusmn;0.01 | 2384 &plusmn;350 | 1.00x | 0.458 |
| restart (no session) | 0.10 &plusmn;0.03 | 2228 &plusmn;521 | 0.93x | 0.458 |
| resume (session management) | 0.56 &plusmn;0.08 | 375 &plusmn;57 | 0.16x | 0.619 |
| parallel, K=1 | 0.69 &plusmn;0.10 | 301 &plusmn;39 | 0.13x | 0.962 |
| parallel, K=4 | 0.53 &plusmn;0.04 | 387 &plusmn;31 | 0.16x | 0.962 |

