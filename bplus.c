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
	int64 NextOffset;

	int64 Root;

	/* Sentinel elements for doubly-linked list of leaves, used for iterators. */
	int64 EndSentinel;
	int64 RendSentinel;
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


Slice	main·Int2Slice(intgo);

#pragma textflag NOSPLIT
intgo
main·Slice2Int(Slice s)
{
	assert(s.len == sizeof(intgo));
	return * (intgo * )s.array;
}


#pragma textflag NOSPLIT
Slice
main·Page2Slice(Page *page)
{
	Slice s;

	s.array = (byte * ) page;
	s.len = s.cap = 1;

	return s;
}


#pragma textflag NOSPLIT
Slice
main·Pages2Bytes(Slice s)
{
	s.len *= PageSize;
	s.cap *= PageSize;
	return s;
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
main·BplusMetaGetEndSentinel(Bplus *t)
{
	return t->Header.EndSentinel;
}


#pragma textflag NOSPLIT
int64
main·BplusMetaGetNextOffset(Bplus *t)
{
	return t->Header.NextOffset;
}


#pragma textflag NOSPLIT
int64
main·BplusMetaGetRendSentinel(Bplus *t)
{
	return t->Header.RendSentinel;
}


#pragma textflag NOSPLIT
int64
main·BplusMetaGetRoot(Bplus *t)
{
	return t->Header.Root;
}


#pragma textflag NOSPLIT
uint64
main·BplusMetaGetVersion(Bplus *t)
{
	return t->Header.Version;
}


#pragma textflag NOSPLIT
void
main·BplusMetaSetEndSentinel(Bplus *t, int64 offset)
{
	t->Header.EndSentinel = offset;
}


#pragma textflag NOSPLIT
void
main·BplusMetaSetNextOffset(Bplus *t, int64 offset)
{
	t->Header.NextOffset = offset;
}


#pragma textflag NOSPLIT
void
main·BplusMetaSetRendSentinel(Bplus *t, int64 offset)
{
	t->Header.RendSentinel = offset;
}


#pragma textflag NOSPLIT
void
main·BplusMetaSetRoot(Bplus *t, int64 offset)
{
	t->Header.Root = offset;
}


#pragma textflag NOSPLIT
void
main·BplusMetaSetVersion(Bplus *t, uint64 version)
{
	t->Header.Version = version;
}


#pragma textflag NOSPLIT
void
main·BplusNodeCopyChildren(Page *dst, Page *src, intgo from, intgo to)
{
	assert(dst->Header.Type == BplusPageTypeNode);
	assert(src->Header.Type == BplusPageTypeNode);

	to = (to == -1) ? src->Header.N.Children : to;
	assert(from < to);
	assert(from > -1);
	assert(from < src->Header.N.Children);
	assert(to <= src->Header.N.Children);

	runtime·memmove(dst->Body.Node.Children, &src->Body.Node.Children[from], (to - from) * sizeof(src->Body.Node.Children[0]));
}


#pragma textflag NOSPLIT
void
main·BplusNodeCopyKeys(Page *dst, Page *src, intgo from, intgo to)
{
	assert(dst->Header.Type == BplusPageTypeNode);
	assert(src->Header.Type == BplusPageTypeNode);

	to = (to == -1) ? src->Header.N.Children : to;
	assert(from < to);
	assert(from < src->Header.N.Children);
	assert(to <= src->Header.N.Children);

	runtime·memmove(dst->Body.Node.Keys, &src->Body.Node.Keys[from], (to - from) * sizeof(src->Body.Node.Keys[0]));
}


#pragma textflag NOSPLIT
intgo
main·BplusNodeFind(Page *p, Slice key)
{
	assert(p->Header.Type == BplusPageTypeNode);

	intgo nkeys = p->Header.N.Children;
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
void
main·BplusNodeInsertChildAt(Page *p, int64 child, intgo index)
{
	assert(p->Header.Type == BplusPageTypeNode);
	assert(p->Header.N.Children < BplusOrder - 2);

	if (index == -1) {
		runtime·memmove(&p->Body.Node.Children[1], &p->Body.Node.Children[0], p->Header.N.Children - 1);
		p->Body.Node.Children[0] = p->Body.Node.ChildPage0;
		p->Body.Node.ChildPage0 = child;
	} else {
		runtime·memmove(&p->Body.Node.Children[index+1], &p->Body.Node.Children[index], (p->Header.N.Children - index) * sizeof(p->Body.Node.Children[0]));
		p->Body.Node.Children[index] = child;
	}

	++p->Header.N.Children;
}


#pragma textflag NOSPLIT
void
main·BplusNodeInsertKeyAt(Page *p, Slice key, intgo index)
{
	assert(p->Header.Type == BplusPageTypeNode);
	assert(p->Header.N.Children < BplusOrder - 2);

	runtime·memmove(&p->Body.Node.Keys[index+1], &p->Body.Node.Keys[index], (p->Header.N.Children - index) * sizeof(p->Body.Node.Keys[0]));
	p->Body.Node.Keys[index] = main·Slice2Int(key);
}


#pragma textflag NOSPLIT
void
main·BplusNodeSetChildAt(Page *p, int64 child, intgo index)
{
	assert(p->Header.Type == BplusPageTypeNode);
	if (index == -1) {
		p->Body.Node.ChildPage0 = child;
	} else {
		p->Body.Node.Children[index] = child;
	}
}


#pragma textflag NOSPLIT
void
main·BplusNodeSetKeyAt(Page *p, Slice key, intgo index)
{
	assert(p->Header.Type == BplusPageTypeNode);
	p->Body.Node.Keys[index] = main·Slice2Int(key);
}


#pragma textflag NOSPLIT
void
main·BplusNodeSetNchildren(Page *p, intgo nchilren)
{
	assert(p->Header.Type == BplusPageTypeNode);
	p->Header.N.Children = nchilren;
}


#pragma textflag NOSPLIT
void
main·BplusLeafCopyKeys(Page *dst, Page *src, intgo from, intgo to)
{
	assert(dst->Header.Type == BplusPageTypeLeaf);
	assert(src->Header.Type == BplusPageTypeLeaf);

	to = (to == -1) ? src->Header.N.Values : to;
	assert(from < to);
	assert(from < src->Header.N.Values);
	assert(to <= src->Header.N.Values);

	runtime·memmove(dst->Body.Leaf.Keys, &src->Body.Leaf.Keys[from], (to - from) * sizeof(src->Body.Leaf.Keys[0]));
}


#pragma textflag NOSPLIT
void
main·BplusLeafCopyValues(Page *dst, Page *src, intgo from, intgo to)
{
	assert(dst->Header.Type == BplusPageTypeLeaf);
	assert(src->Header.Type == BplusPageTypeLeaf);

	to = (to == -1) ? src->Header.N.Values : to;
	assert(from < to);
	assert(from < src->Header.N.Values);
	assert(to <= src->Header.N.Values);

	runtime·memmove(dst->Body.Leaf.Values, &src->Body.Leaf.Values[from], (to - from) * sizeof(src->Body.Leaf.Values[0]));
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
Slice
main·BplusLeafGetKeyAt(Page *p, intgo index)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	return main·Int2Slice(p->Body.Leaf.Keys[index]);
}


#pragma textflag NOSPLIT
int64
main·BplusLeafGetNext(Page *p)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	return p->Body.Leaf.Next;
}


#pragma textflag NOSPLIT
intgo
main·BplusLeafGetNvalues(Page *p)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	return p->Header.N.Values;
}


#pragma textflag NOSPLIT
int64
main·BplusLeafGetPrev(Page *p)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	return p->Body.Leaf.Prev;
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
void
main·BplusLeafInsertKeyAt(Page *p, Slice key, intgo index)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	assert(p->Header.N.Values < BplusOrder - 1);

	runtime·memmove(&p->Body.Leaf.Keys[index+1], &p->Body.Leaf.Keys[index], (p->Header.N.Values - index) * sizeof(p->Body.Leaf.Keys[0]));
	p->Body.Leaf.Keys[index] = main·Slice2Int(key);
}


#pragma textflag NOSPLIT
void
main·BplusLeafInsertValueAt(Page *p, Slice value, intgo index)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	assert(p->Header.N.Values < BplusOrder - 1);

	runtime·memmove(&p->Body.Leaf.Values[index+1], &p->Body.Leaf.Values[index], (p->Header.N.Values - index) * sizeof(p->Body.Leaf.Values[0]));
	p->Body.Leaf.Values[index] = main·Slice2Int(value);

	++p->Header.N.Values;
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


#pragma textflag NOSPLIT
void
main·BplusLeafSetNvalues(Page *p, intgo nvalues)
{
	assert(p->Header.Type == BplusPageTypeLeaf);
	p->Header.N.Values = nvalues;
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


