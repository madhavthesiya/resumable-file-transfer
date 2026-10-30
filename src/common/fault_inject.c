#include "common.h"
#include "fault_inject.h"
#include "logging.h"

#include <stdlib.h>
#include <time.h>
#include <unistd.h>

static unsigned g_seed;
/* Worker index handed out by fault_init(); _Atomic because several threads may
 * be starting at the same moment. */
static _Atomic unsigned g_worker_no;

void fault_seed_init(unsigned seed_or_zero)
{
    if (seed_or_zero != 0) {
        g_seed = seed_or_zero;
        log_info("fault injection: seed %u (pinned with --seed)", g_seed);
    } else {
        g_seed = (unsigned)time(NULL) ^ ((unsigned)getpid() << 16);
        log_info("fault injection: seed %u (derived; pass --seed %u to replay)",
                 g_seed, g_seed);
    }
}

unsigned fault_seed_get(void) { return g_seed; }

void fault_init(fault_rng_t *f, double p)
{
    unsigned idx = g_worker_no++;
    /* Knuth multiplicative hash of the worker index, mixed into the process
     * seed: cheap, and keeps the streams of workers 0,1,2,... far apart
     * instead of merely offset by one. */
    f->state  = g_seed ^ (idx * 2654435761u);
    f->p      = p;
    f->checks = 0;
    f->fires  = 0;
}

int fault_should_trigger(fault_rng_t *f)
{
    if (f->p <= 0.0)
        return 0;
    f->checks++;
    /* rand_r() yields 0..RAND_MAX inclusive; dividing by RAND_MAX+1 gives a
     * value in [0,1), so the comparison fires with probability exactly p
     * to within the generator's resolution. */
    double r = (double)rand_r(&f->state) / ((double)RAND_MAX + 1.0);
    if (r < f->p) {
        f->fires++;
        return 1;
    }
    return 0;
}
