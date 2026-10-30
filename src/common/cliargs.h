/*
 * cliargs.h - parsing of the optional trailing arguments.
 *
 * HOW THIS WORKS
 * --------------
 * The assignment fixes the mandatory positional arguments exactly:
 *
 *     ./server <port> <topics_root_dir> [<failure_probability>]
 *     ./client <server_host> <server_port> <topic> <output_dir>
 *
 * Everything else this project exposes is an *optional trailing* argument with
 * a compiled-in default, so both mandatory forms always work unmodified.  Each
 * program declares a small table of opt_spec_t and calls opts_parse() on
 * whatever argv entries follow its positionals.
 *
 * Unknown options are a hard error rather than being ignored: a typo in a
 * benchmark script should fail loudly, not silently measure the default.
 */
#ifndef IT305_CLIARGS_H
#define IT305_CLIARGS_H

#include <stdint.h>
#include <stdio.h>

typedef enum {
    OPT_INT,     /* int      */
    OPT_UINT,    /* unsigned */
    OPT_U64,     /* uint64_t */
    OPT_DOUBLE,  /* double   */
    OPT_STR,     /* const char * (points into argv; no copy)   */
    OPT_FLAG     /* int, set to 1; consumes no following value */
} opt_kind_t;

typedef struct {
    const char *name;   /* without the leading "--"        */
    opt_kind_t  kind;
    void       *dst;
    const char *help;
} opt_spec_t;

/* Parse argv[first..argc-1] against `specs`.
 * Returns 0 on success, -1 on an unknown option, a missing value or a value
 * that does not parse (the reason is printed to stderr). */
int opts_parse(int argc, char **argv, int first,
               const opt_spec_t *specs, size_t nspecs);

/* Print "  --name <kind>   help" for every spec, for usage text. */
void opts_usage(const opt_spec_t *specs, size_t nspecs, FILE *out);

/* Strict scalar parsers for the mandatory positionals.  Each returns 0 on
 * success, -1 if the text is not a complete, in-range value.  They reject
 * trailing junk ("8080x") that strtol() would otherwise accept. */
int parse_port(const char *s, uint16_t *out);
int parse_prob(const char *s, double *out);     /* must land in [0,1] */
int parse_u64 (const char *s, uint64_t *out);

#endif /* IT305_CLIARGS_H */
