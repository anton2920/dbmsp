#include "runtime.h"
#include "textflag.h"

#define PageSize 4096

#define BplusHeaderSize (sizeof(BplusHeader))
#define BplusPadSize (PageSize-BplusHeaderSize)
#define BplusOrder (1<<(sizeof(uint8)*8))

typedef struct BplusHeader BplusHeader;
typedef struct Bplus Bplus;

#define PageHeaderSize (sizeof(PageHeader))
#define PageBodySize (PageSize-PageHeaderSize)

typedef struct PageHeader PageHeader;
typedef struct Node Node;
typedef struct Leaf Leaf;
typedef union PageBody PageBody;
typedef struct Page Page;

struct BplusHeader {
	uint64 Version;

	int64 Root;
	int64 Blobs;
	int64 FreeList;
	int64 Snapshots;

	/* Sentinel elements for doubly-linked list of leaves, used for iterators. */
	int64 EndSentinel;
	int64 RendSentinel;
};

struct Bplus {
	BplusHeader Header;
	byte Pad[BplusPadSize];
};

struct PageHeader {
	uint8 Type;
	union {
		uint8 Children;
		uint8 Values;
	} N;
	uint8 RefCount;

	uint8 Pad[5];
};

struct Node {
	/* Keys are either '~(1<<63)&int63' direct value or '(1<<63)|int63' offset to an actual key. */
	uint64 Keys[BplusOrder-1];

	int64 Children[BplusOrder-1];
	int64 ChildPage0;
};

struct Leaf {
	uint64 Keys[BplusOrder-2];
	uint64 Values[BplusOrder-2];

	int64 Prev;
	int64 Next;
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
	Node node;
	Leaf leaf;
	Bplus t;

	runtime·printf("BplusSize = %d, BplusHeaderSize = %d\n", sizeof(t), sizeof(t.Header));
	runtime·printf("PageSize = %d, PageHeaderSize = %d, PageBodySize = %d\n", sizeof(Page), sizeof(PageHeader), sizeof(PageBody));
	runtime·printf("NodeSize= %d, Node->KeysSize = %d, Node->ChildrenSize = %d\n", sizeof(node), sizeof(node.Keys), sizeof(node.Children));
	runtime·printf("LeafSize = %d, Leaf->KeysSize = %d, Leaf->ValuesSize = %d\n", sizeof(leaf), sizeof(leaf.Keys), sizeof(leaf.Values));
}


#pragma textflag NOSPLIT
void
main·CallC(void *_fn)
{
	void (**fn)(void) = _fn;
	runtime·onM(fn);
}


