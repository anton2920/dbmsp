#include "runtime.h"
#include "textflag.h"

#include "assert.h"

/* NOTE(anton2920): must be in sync with Go definition. */
#define PageSize 4096

#define BplusHeaderSize (sizeof(BplusHeader))
#define BplusPadSize (PageSize-BplusHeaderSize)
#define BplusOrder (1<<(sizeof(uint8)*8))
#define BplusVersion 0x1

typedef struct BplusHeader BplusHeader;
typedef struct Bplus Bplus;

#define PageHeaderSize (sizeof(PageHeader))
#define PageBodySize (PageSize-PageHeaderSize)

typedef struct PageHeader PageHeader;
typedef struct Node Node;
typedef struct Leaf Leaf;
typedef struct LeafFindResult LeafFindResult;
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

	int64 NextOffset;
};

struct Bplus {
	BplusHeader Header;
	byte Pad[BplusPadSize];
};

/* NOTE(anton2920): must be in sync with Go definition. */
enum {
	BplusPageTypeNone,
	BplusPageTypeNode,
	BplusPageTypeLeaf,
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
	/* Keys are either '~(1<<63)&int63' direct value or '(1<<63)|int63' offset to an actual key? */
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

struct LeafFindResult {
	intgo Index;
	bool OK;
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


#pragma textflag NOSPLIT
Slice
main·Int2Slice(intgo x)
{
	Slice s;

	s.array = (byte * ) & x;
	s.len = s.cap = sizeof(x);

	return s;
}


#pragma textflag NOSPLIT
intgo
main·Slice2Int(Slice s)
{
	return * (intgo * )s.array;
}


#pragma textflag NOSPLIT
int64
main·BplusGetEndSentinel(Bplus *t)
{
	return t->Header.EndSentinel;
}


#pragma textflag NOSPLIT
int64
main·BplusGetRendSentinel(Bplus *t)
{
	return t->Header.RendSentinel;
}


#pragma textflag NOSPLIT
int64
main·BplusGetRootOffset(Bplus *t)
{
	return t->Header.Root;
}


#pragma textflag NOSPLIT
void
main·BplusSetEndSentinel(Bplus *t, int64 offset)
{
	t->Header.EndSentinel = offset;
}


#pragma textflag NOSPLIT
void
main·BplusSetRendSentinel(Bplus *t, int64 offset)
{
	t->Header.RendSentinel = offset;
}


#pragma textflag NOSPLIT
void
main·BplusSetRootOffset(Bplus *t, int64 offset)
{
	t->Header.Root = offset;
}


#pragma textflag NOSPLIT
uint8
main·BplusPageGetType(Page *p)
{
	return p->Header.Type;
}


#pragma textflag NOSPLIT
void
main·BplusPageInit(Page *p, uint8 type, intgo n)
{
	runtime·memclr((byte * )p, PageSize);

	p->Header.Type = type;
	switch (type) {
	case BplusPageTypeNode:
		p->Header.N.Children = n;
		break;
	case BplusPageTypeLeaf:
		p->Header.N.Values = n;
		break;
	}
}


#pragma textflag NOSPLIT
int64
main·BplusNodeGetChildAt(Page *p, intgo index)
{
	assert(p->Header.Type == BplusPageTypeNode);
	if (index == -1)
		return p->Body.Node.ChildPage0;
	return p->Body.Node.Children[index];
}


/* TODO(anton2920): add support for variable length keys. */
#pragma textflag NOSPLIT
Slice
main·BplusNodeGetKeyAt(Page *p, intgo index)
{
	assert(p->Header.Type == BplusPageTypeNode);
	return main·Int2Slice(p->Body.Node.Keys[index]);
}


#pragma textflag NOSPLIT
intgo
main·BplusNodeGetNchildren(Page *p)
{
	assert(p->Header.Type == BplusPageTypeNode);
	return p->Header.N.Children;
}


#pragma textflag NOSPLIT
intgo
main·BplusNodeFind(Page *p, Slice key)
{
	assert(p->Header.Type == BplusPageTypeNode);

	intgo nkeys = p->Header.N.Children - 1;
	uint64 k = *((uint64 * )key.array);
	Node * n = &p->Body.Node;
	intgo i;

	if (k >= n->Keys[nkeys-1]) {
		return nkeys - 1;
	}
	for (i = 0; i < nkeys; ++i) {
		if (k < n->Keys[i]) {
			return i - 1;
		}
	}
	return nkeys - 1;
}


#pragma textflag NOSPLIT
void
main·BplusNodeSetChildAt(Page *p, int64 child, intgo index)
{
	assert(p->Header.Type == BplusPageTypeNode);
	if (index == -1)
		p->Body.Node.ChildPage0 = child;
	else
		p->Body.Node.Children[index] = child;
}


/* TODO(anton2920): add support for variable length keys. */
#pragma textflag NOSPLIT
Slice
main·BplusLeafGetKeyAt(Page *p, intgo index)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	return main·Int2Slice(p->Body.Leaf.Keys[index]);
}


/* TODO(anton2920): add support for variable length values. */
#pragma textflag NOSPLIT
Slice
main·BplusLeafGetValueAt(Page *p, intgo index)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	return main·Int2Slice(p->Body.Leaf.Values[index]);
}


#pragma textflag NOSPLIT
intgo
main·BplusLeafGetNvalues(Page *p)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	return p->Header.N.Values;
}


#pragma textflag NOSPLIT
LeafFindResult
main·BplusLeafFind(Page *p, Slice key)
{
	assert(p->Header.Type == BplusPageTypeLeaf);

	LeafFindResult result;
	intgo nkeys = p->Header.N.Values;
	uint64 k = *((uint64 * )key.array);
	Leaf * l = &p->Body.Leaf;
	intgo i;

	if (nkeys == 0) {
		result.Index = -1;
		result.OK = false;
		return result;
	}
	if (k >= l->Keys[nkeys-1]) {
		bool eq = k == l->Keys[nkeys-1];
		result.Index = nkeys - 1 - eq;
		result.OK = eq;
		return result;
	}
	for (i = 0; i < nkeys; ++i) {
		if (k <= l->Keys[i]) {
			result.Index = i - 1;
			result.OK = k == l->Keys[i];
			return result;
		}
	}

	result.Index = nkeys;
	result.OK = false;
	return result;
}


/* TODO(anton2920): add support for variable length keys. */
#pragma textflag NOSPLIT
void
main·BplusLeafSetKeyAt(Page *p, Slice key, intgo index)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	p->Body.Leaf.Keys[index] = main·Slice2Int(key);
}


#pragma textflag NOSPLIT
void
main·BplusLeafSetNext(Page *p, int64 next)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	p->Body.Leaf.Next = next;
}


#pragma textflag NOSPLIT
void
main·BplusLeafSetPrev(Page *p, int64 prev)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	p->Body.Leaf.Prev = prev;
}


/* TODO(anton2920): add support for variable length values. */
#pragma textflag NOSPLIT
void
main·BplusLeafSetValueAt(Page *p, Slice value, intgo index)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	p->Body.Leaf.Values[index] = main·Slice2Int(value);
}


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

	assert(sizeof(Page) == PageSize);
}


#pragma textflag NOSPLIT
void
main·CallC(void *_fn)
{
	void (**fn)(void) = _fn;
	runtime·onM(fn);
}


