#ifndef DEFINITIONS_H
#define DEFINITIONS_H

#ifndef MAX
#define MAX(a, b) (((a) < (b)) ? (b) : (a))
#endif

#ifndef MIN
#define MIN(a, b) (((a) < (b)) ? (a) : (b))
#endif

/* Check if n is a power of 2 */
#define ISPOWOF2(n)     ((n != 0) && ((n & (n - 1)) == 0))

/* Bitwise operations */
#define GETBIT(n, i)    ((n >> (63UL - i)) & 1UL)
#define SETBIT(n, i)    n = (n | (1UL << (63UL - i)))
#define CLEARBIT(n, i)  n = (n & ~(1UL << (63UL - i)))

#define GETBIT16(n, i)  ((n >> (15UL - i)) & 1UL)
#define SETBIT16(n, i)  n = (n | (1UL << (15UL - i)))

#define BITS2BLOCKS(bits) \
    (((bits) % 64 == 0) ? ((bits) / 64) : (((bits) / 64) + 1))

#endif
