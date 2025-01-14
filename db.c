#include "runtime.h"
#include "textflag.h"

#define PageSize 4096
#define PageHeaderSize (sizeof(PageHeader))
#define PageBodySize (PageSize-PageHeaderSize)

typedef struct Bplus Bplus;
typedef struct PageHeader PageHeader;
typedef struct Node Node;
typedef struct Leaf Leaf;
typedef union PageBody PageBody;
typedef struct Page Page;

struct Bplus {
	int64 RootOffset;
};

struct PageHeader {
	uint8 Type;
	uint8 Pad[7];
};

struct Node {
	uint64 Stub;
};

struct Leaf {
	uint64 Stub;
};

union PageBody {
	Node Node;
	Leaf Leaf;
	byte OpaqueData[PageBodySize];
};

struct Page {
	PageHeader Header;
	PageBody Body;
};

void
main·Main()
{
	runtime·printf("PageSize = %d, PageHeaderSize = %d, PageBodySize = %d\n", sizeof(Page), sizeof(PageHeader), sizeof(PageBody));
}


#pragma textflag NOSPLIT
void
main·CallC(void *_fn)
{
	void (**fn)(void) = _fn;
	runtime·onM(fn);
}


