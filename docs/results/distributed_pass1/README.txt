First three-machine pass, retained for transparency - NOT the reported data.

All 40 runs completed with status=ok and crc_fail=0, so the data is valid, but
the experiment design was biased in two ways that were corrected before the
reported run:

  1. No page-cache warm-up. The earliest runs read the topic from disk while
     later ones were served from RAM.
  2. All single-threaded runs executed before all multi-threaded runs, so the
     single-threaded mode absorbed the cold-cache penalty and any drift in
     wireless conditions over the session.

Together these made the multi-threaded server look 2.13x faster at one client.
With the cache warmed and the two modes interleaved within each repeat, the
same measurement gives 1.23x. The corrected run is in Results/distributed/ and
is what the report presents.
