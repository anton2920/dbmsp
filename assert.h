#ifndef ASSERT_H
#define ASSERT_H

void _assert(byte *expr);

#ifdef DEBUG
#define	assert(x)	if(x){}else _assert((byte*)"x")
#else
#define	assert(x)
#endif

#endif /* ASSERT_H */
