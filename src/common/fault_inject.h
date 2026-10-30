/*
 * fault_inject.h - the server-side probabilistic connection drop of Part II.
 *
 * HOW THIS WORKS
 * --------------
 * The server takes a failure probability p on the command line.  After every
 * data chunk it writes, a worker draws a uniform random number and, with
 * probability p, abandons the connection: it closes the socket without sending
 * RESP_DONE, so the client observes a mid-transfer disconnection.
 *
 * WHAT p MEANS - read this before interpreting any measurement
 * ------------
 * p is the probability of failure **per emitted 16 KiB data chunk**, not per
 * transfer and not per second.  A topic made of C chunks therefore completes
 * without a fault with probability (1-p)^C, and the expected number of faults
 * in one full pass is C*p.  Because the real dataset's largest topic is about
 * 4350 chunks, p = 0.01 would mean roughly 43 faults per pass, which is why the
 * experiments use much smaller values.  README.md gives the exact figures.
 *
 * Each worker thread owns its own rand_r() state, so no two workers share or
 * contend on RNG state.  Per-worker seeds are derived from one process-wide
 * seed, which is logged at start-up and can be pinned with --seed for a
 * reproducible fault pattern.
 */
#ifndef IT305_FAULT_INJECT_H
#define IT305_FAULT_INJECT_H

#include <stdint.h>

typedef struct {
    unsigned state;    /* rand_r() state, private to one worker  */
    double   p;        /* failure probability per chunk          */
    uint64_t checks;   /* how many times the decision was made   */
    uint64_t fires;    /* how many times it came out "fail now"  */
} fault_rng_t;

/* Establish the process-wide seed.  Pass 0 to derive one from the wall clock
 * and the pid; pass a non-zero value (from --seed) to pin it.  The effective
 * seed is always logged, so a run can be replayed even if --seed was not
 * given.  Call once, before any worker threads are created. */
void     fault_seed_init(unsigned seed_or_zero);
unsigned fault_seed_get(void);

/* Initialise one worker's generator.  Each call consumes a distinct index from
 * a process-wide atomic counter, so workers get different streams; with a
 * pinned seed the n-th worker of a run always gets the same stream. */
void fault_init(fault_rng_t *f, double p);

/* Returns 1 if this chunk boundary should drop the connection, else 0.
 * Always returns 0 when p <= 0, so Part II binaries run fault-free at p=0
 * and can be compared directly against Part I. */
int fault_should_trigger(fault_rng_t *f);

/* Expected faults per full pass over `chunks` chunks, for reporting. */
static inline double fault_expected(double p, uint64_t chunks)
{
    return p * (double)chunks;
}

#endif /* IT305_FAULT_INJECT_H */
