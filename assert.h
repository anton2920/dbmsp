#ifndef ASSERT_H
#define ASSERT_H

void _assert(byte *expr);

#define	assert(x)	if(x){}else _assert((byte*)"x")

#endif /* ASSERT_H */
