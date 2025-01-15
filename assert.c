#include "runtime.h"
#include "textflag.h"


#pragma textflag NOSPLIT
void
_assert(byte *expr)
{
	static byte	buffer[1024];

	runtime·snprintf(buffer, sizeof(buffer), "assert failed: %s", expr);
	runtime·throw((int8 * )buffer);
}


