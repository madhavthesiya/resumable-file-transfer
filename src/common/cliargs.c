#include "common.h"
#include "cliargs.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

/* Convert `text` according to `kind` and store it through `dst`.
 * Returns 0 on success, -1 if the text does not parse. */
static int store(const opt_spec_t *s, const char *text)
{
    char *end = NULL;
    errno = 0;

    switch (s->kind) {
    case OPT_INT: {
        long v = strtol(text, &end, 10);
        if (errno || !end || *end || v < INT_MIN || v > INT_MAX) return -1;
        *(int *)s->dst = (int)v;
        return 0;
    }
    case OPT_UINT: {
        unsigned long v = strtoul(text, &end, 10);
        if (errno || !end || *end || v > UINT_MAX) return -1;
        *(unsigned *)s->dst = (unsigned)v;
        return 0;
    }
    case OPT_U64: {
        unsigned long long v = strtoull(text, &end, 10);
        if (errno || !end || *end) return -1;
        *(uint64_t *)s->dst = (uint64_t)v;
        return 0;
    }
    case OPT_DOUBLE: {
        double v = strtod(text, &end);
        if (errno || !end || *end) return -1;
        *(double *)s->dst = v;
        return 0;
    }
    case OPT_STR:
        *(const char **)s->dst = text;
        return 0;
    case OPT_FLAG:
        *(int *)s->dst = 1;
        return 0;
    }
    return -1;
}

int opts_parse(int argc, char **argv, int first,
               const opt_spec_t *specs, size_t nspecs)
{
    for (int i = first; i < argc; i++) {
        const char *a = argv[i];
        if (a[0] != '-' || a[1] != '-') {
            fprintf(stderr, "error: unexpected argument '%s' "
                            "(optional settings use --name value)\n", a);
            return -1;
        }
        const char *name = a + 2;

        const opt_spec_t *hit = NULL;
        for (size_t k = 0; k < nspecs; k++)
            if (strcmp(specs[k].name, name) == 0) { hit = &specs[k]; break; }

        if (!hit) {
            fprintf(stderr, "error: unknown option '--%s'\n", name);
            return -1;
        }
        if (hit->kind == OPT_FLAG) {
            store(hit, NULL);
            continue;
        }
        if (i + 1 >= argc) {
            fprintf(stderr, "error: option '--%s' needs a value\n", name);
            return -1;
        }
        if (store(hit, argv[++i]) < 0) {
            fprintf(stderr, "error: bad value '%s' for option '--%s'\n",
                    argv[i], name);
            return -1;
        }
    }
    return 0;
}

void opts_usage(const opt_spec_t *specs, size_t nspecs, FILE *out)
{
    static const char *kind_text[] = { "<int>", "<uint>", "<uint64>",
                                       "<float>", "<string>", "" };
    for (size_t k = 0; k < nspecs; k++)
        fprintf(out, "  --%-10s %-9s %s\n", specs[k].name,
                kind_text[specs[k].kind], specs[k].help);
}

int parse_port(const char *s, uint16_t *out)
{
    char *end = NULL;
    errno = 0;
    unsigned long v = strtoul(s, &end, 10);
    /* Port 0 would make the kernel pick an ephemeral port, which the client
     * could never predict, so it is rejected as well. */
    if (errno || !end || *end || v == 0 || v > 65535)
        return -1;
    *out = (uint16_t)v;
    return 0;
}

int parse_prob(const char *s, double *out)
{
    char *end = NULL;
    errno = 0;
    double v = strtod(s, &end);
    if (errno || !end || *end || !(v >= 0.0 && v <= 1.0))
        return -1;
    *out = v;
    return 0;
}

int parse_u64(const char *s, uint64_t *out)
{
    char *end = NULL;
    errno = 0;
    unsigned long long v = strtoull(s, &end, 10);
    if (errno || !end || *end)
        return -1;
    *out = (uint64_t)v;
    return 0;
}
