[![Continuous Integration](https://github.com/weso/sparkwdsub/actions/workflows/ci.yml/badge.svg)](https://github.com/weso/sparkwdsub/actions/workflows/ci.yml)

# sparkwdsub

Spark processing of wikidata subsets using Shape Expressions

This repo contains an example script that processes Wikidata subsets using Shape Expressions


# Command line

```
Usage: sparkwdsub dump --schema <file> [--out <file>] [--site <string>] [--maxIterations <integer>] [--verbose] [--loggingLevel <string>] <dumpFile>
 Process example dump file.
 Options and flags:
     --help
         Display this help text.
     --schema <file>, -s <file>
         schema path
     --out <file>, -o <file>
         output path
     --site <string>
         Base url, default =http://www.wikidata.org/entity
     --maxIterations <integer>
        Max iterations for Pregel algorithm, default =20
     --verbose
         Verbose mode
     --loggingLevel <string>
         Logging level (ERROR, WARN, INFO), default=ERROR
```

Example:

```
sparkwdsub dump -s examples/cities.shex examples/6lines.json
```

