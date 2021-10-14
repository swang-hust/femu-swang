#ifndef __AVLTREE__
#define __AVLTREE__

#include <string.h>
#include <stdint.h>
#include <stdbool.h>

#define AVL_NULL		(TREE_NODE *)0

#define EH_FACTOR	0
#define LH_FACTOR	1
#define RH_FACTOR	-1
#define LEFT_MINUS	0
#define RIGHT_MINUS	1


#define ORDER_LIST_WANTED

#define INSERT_PREV	0
#define INSERT_NEXT	1

typedef struct _AVL_TREE_NODE
{
#ifdef ORDER_LIST_WANTED
	struct _AVL_TREE_NODE *prev;
	struct _AVL_TREE_NODE *next;
#endif
	struct _AVL_TREE_NODE *tree_root;
	struct _AVL_TREE_NODE *left_child;
	struct _AVL_TREE_NODE *right_child;
	int  bf;  /* Balance factor; when the absolute value of the balance factor is greater than or equal to 2, the tree is unbalanced */
}TREE_NODE;

typedef struct buffer_info
{
	struct buffer_group *buffer_head;            /*as LRU head which is most recently used*/
	struct buffer_group *buffer_tail;            /*as LRU tail which is least recently used*/
	TREE_NODE	*pTreeHeader;     				 /*for search target lsn is LRU table*/
	uint32_t count;	/* Nodes count */

#ifdef ORDER_LIST_WANTED
	TREE_NODE	*pListHeader;
	TREE_NODE	*pListTail;
#endif
	int 	(*keyCompare)(TREE_NODE * , TREE_NODE *);
	int		(*free)(TREE_NODE *);

	/* Statistics can be added here */
	uint32_t max_secs;
	uint32_t secs_cnt;
} tAVLTree;


int avlTreeHigh(TREE_NODE *);
int avlTreeCheck(tAVLTree *,TREE_NODE *);
void R_Rotate(TREE_NODE **);
void L_Rotate(TREE_NODE **);
void LeftBalance(TREE_NODE **);
void RightBalance(TREE_NODE **);
int avlDelBalance(tAVLTree *,TREE_NODE *,int);
void AVL_TREE_LOCK(tAVLTree *,int);
void AVL_TREE_UNLOCK(tAVLTree *);
void AVL_TREENODE_FREE(tAVLTree *,TREE_NODE *);

#ifdef ORDER_LIST_WANTED
int orderListInsert(tAVLTree *,TREE_NODE *,TREE_NODE *,int);
int orderListRemove(tAVLTree *,TREE_NODE *);
TREE_NODE *avlTreeFirst(tAVLTree *);
TREE_NODE *avlTreeLast(tAVLTree *);
TREE_NODE *avlTreeNext(TREE_NODE *pNode);
TREE_NODE *avlTreePrev(TREE_NODE *pNode);
#endif

int avlTreeInsert(tAVLTree *,TREE_NODE **,TREE_NODE *,int *);
int avlTreeRemove(tAVLTree *,TREE_NODE *);
TREE_NODE *avlTreeLookup(tAVLTree *,TREE_NODE *,TREE_NODE *);
tAVLTree *avlTreeCreate(int *,int *);
int avlTreeDestroy(tAVLTree *);
int avlTreeFlush(tAVLTree *);
int avlTreeAdd(tAVLTree *,TREE_NODE *);
int avlTreeDel(tAVLTree *,TREE_NODE *);
TREE_NODE *avlTreeFind(tAVLTree *,TREE_NODE *);
unsigned int avlTreeCount(tAVLTree *);


#endif //__AVLTREE__
