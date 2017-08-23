crcspeed
========

CRC be slow.

This make CRC be fast.

No original ideas, but original adaptations.  Lots of shoulder standing.

This started out as a modified version of comment at http://stackoverflow.com/questions/20562546
then was made more extensible.

Features
--------

  - CRC processing in 8-byte steps for CRC-64 (Jones) and CRC-16 (CCITT).
  - Generates CRCs with overhead of 1.5 CPU cycles per byte
  - Little endian and big endian support
    - big endian support hasn't been tested yet (because `qemu-system-sparc` hates me).
  - Test suite generates comparison for: bit-by-bit calculation, byte-by-byte calcuation
  (Sarwate / lookup table), and 8-bytes-at-once calculation.  Results are reported
  with resulting CRCs, throughput, and CPU cycles per byte comparisons.

Usage
-----

  - Use little endian CRCs:
    - `crc64speed_init();`
    - `crc64speed(old_crc, new_data, new_data_length);`
    - `crc16speed_init();`
    - `crc16speed(old_crc, new_data, new_data_length);`
  - Use native architecture CRCs:
    - `crc64speed_init_native();`
    - `crc64speed_native(old_crc, new_data, new_data_length);`
    - `crc16speed_init_native();`
    - `crc16speed_native(old_crc, new_data, new_data_length);`
  - Use custom CRC64 variant:
    - `crcspeed64native_init(crc_calculation_function, uint64_t populate[8][256]);`
      - crc calculation function takes (0, data, data_len) and returns crc64 as `uint64_t`.
      - `populate` is a lookup table _init populates for future lookups.
    - `crcspeed64native(populated_lookup_table, old_crc, new_data, new_data_length);`
  - Use custom CRC16 parameters:
    - `crcspeed16native_init(crc_calculation_function, uint16_t populate[8][256]);`
      - crc calculation function takes (0, data, data_len) and returns crc16 as `uint16_t`.
    - `crcspeed16native(populated_lookup_table, old_crc, new_data, new_data_length);`

Additionally, there are specific functions for forcing little or big endian calculations:
`crcspeed64little_init()`, `crcspeed64little()`, `crc64big_init()`, `crcspeed64big()`,
`crcspeed16little_init()`, `crcspeed16little()`, `crc16big_init()`, `crcspeed16big()`.

Architecture
------------

  - `crcspeed.c` is a _framework_ for bootstrapping a fast lookup table using an existing function
  used to return the CRC for byte values 0 to 255.  Lookups then use fast lookup table to
  calculate CRCs 8 bytes per loop iteration.
  - `crc64speed.c` is a ready-to-use fast, self-contained CRC-64-Jones implementation.
  - `crc16speed.c` is a ready-to-use fast, self-contained CRC16-CCITT implementation.
  - when in a multithreaded environment, do not run initialization function(s) in parallel.
  - for fastest CRC calculations, you can force the entire CRC lookup table into
  CPU caches by running `crc64speed_cache_table()` or `crc16speed_cache_table()`.
  Those functions just iterate over the lookup table to bring everything into local
  caches out from main memory (or worse, paged out to disk).
  - The CRC-16 lookup table is 4 KB (8x256 16 bit entries = 8 * 256 * 2 bytes = 4096 bytes).
  - The CRC-64 lookup table is 16 KB (8x256 64 bit entires = 8 * 256 * 8 bytes = 16384 bytes).

Benchmark
---------

The Makefile builds three test excutables:
  - `crc64speed-test` just returns check values for two input types across all
  three internal CRC process methods (bit-by-bit, byte-by-byte, 8-bytes-at-once).
  - `crc16speed-test` returns check values for the same data, except limited to CRC16 results.
  - `crcspeed-test` has two options:
    - no arguments: return check values for crc64 and crc16 at the same time.
    - one argument: filename of file to read into memory then run CRC tests against.
      - If CRC results do not match (for each CRC variant), the return value of
      `crcspeed-test` is 1, otherwise 0 on success.

```haskell
% make
    CC crcspeed-test
    CC crc64speed-test
    CC crc16speed-test
% ./crcspeed-test ~/Downloads/John\ Mayer\ -\ Live\ At\ Austin\ City\ Limits\ PBS\ -\ Full\ Concert-gcdUz12FkdQ.mp4 
Comparing CRCs against 730.72 MB file...

crc64 (no table)
CRC = ee43263b0a2b6c60
7.142642 seconds at 102.30 MB/s (24.18 CPU cycles per byte)

crc64 (lookup table)
CRC = ee43263b0a2b6c60
1.777920 seconds at 411.00 MB/s (6.02 CPU cycles per byte)

crc64speed
CRC = ee43263b0a2b6c60
0.448819 seconds at 1628.09 MB/s (1.52 CPU cycles per byte)

crc16 (no table)
CRC = 000000000000490f
7.413062 seconds at 98.57 MB/s (25.10 CPU cycles per byte)

crc16 (lookup table)
CRC = 000000000000490f
1.951917 seconds at 374.36 MB/s (6.61 CPU cycles per byte)

crc16speed
CRC = 000000000000490f
0.441418 seconds at 1655.38 MB/s (1.49 CPU cycles per byte)
```

License
-------
All work here is released under BSD or Apache 2.0 License or equivalent.

Thanks
------
Thanks to Mark Adler for providing a readable implementation of slicing-by-8 in a  [stackoverflow comment](http://stackoverflow.com/questions/20562546/how-to-get-crc64-distributed-calculation-use-its-linearity-property/20579405#20579405).

Thanks for [pycrc](https://github.com/tpircher/pycrc) for saving me another month figuring out how to write CRC-64-Jones by hand.

Thanks to [A PAINLESS GUIDE TO CRC ERROR DETECTION ALGORITHMS](http://www.zlib.net/crc_v3.txt) for providing so many details it was clear I should give up and not try to re-create everything myself.
