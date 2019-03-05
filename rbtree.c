/*-------------------------------------------------------------------------
 *
 * rbtree.c
 *	  implementation for PostgreSQL generic Red-Black binary tree package
 *	  Adopted from http://algolist.manual.ru/ds/rbtree.php
 *
 * This code comes from Thomas Niemann's "Sorting and Searching Algorithms:
 * a Cookbook".
 *
 * See http://www.cs.auckland.ac.nz/software/AlgAnim/niemann/s_man.htm for
 * license terms: "Source code, when part of a software project, may be used
 * freely without reference to the author."
 *
 * Red-black trees are a type of balanced binary tree wherein (1) any child of
 * a red node is always black, and (2) every path from root to leaf traverses
 * an equal number of black nodes.  From these properties, it follows that the
 * longest path from root to leaf is only about twice as long as the shortest,
 * so lookups are guaranteed to run in O(lg n) time.
 *
 * Copyright (c) 2009-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/lib/rbtree.c
 *
 *-------------------------------------------------------------------------
 */

#include "rbtree.h"
#include <assert.h>

/*
 * Colors of nodes (values of RBNode.color)
 */
#define RBBLACK		(0)
#define RBRED		(1)

/*
 * all leafs are sentinels, use customized NIL name to prevent
 * collision with system-wide constant NIL which is actually NULL
 */
#define RBNIL (&sentinel)

static RBNode sentinel = {RBNIL, RBNIL, NULL, RBBLACK};


/*
 * rb_create: create an empty RBTree
 *
 * Arguments are:
 *  tree: buffer for storing RBTree structure
 *	node_size: actual size of tree nodes (> sizeof(RBNode))
 *	The manipulation functions:
 *	comparator: compare two RBNodes for less/equal/greater
 *	combiner: merge an existing tree entry with a new one
 *	allocfunc: allocate a new RBNode
 *	freefunc: free an old RBNode
 *	arg: passthrough pointer that will be passed to the manipulation functions
 *
 * Note that the combiner's righthand argument will be a "proposed" tree node,
 * ie the input to rb_insert, in which the RBNode fields themselves aren't
 * valid.  Similarly, either input to the comparator may be a "proposed" node.
 * This shouldn't matter since the functions aren't supposed to look at the
 * RBNode fields, only the extra fields of the struct the RBNode is embedded
 * in.
 *
 * The freefunc should just be pfree or equivalent; it should NOT attempt
 * to free any subsidiary data, because the node passed to it may not contain
 * valid data!	freefunc can be NULL if caller doesn't require retail
 * space reclamation.
 *
 * Since tree contents are managed by the caller, there is currently not
 * an explicit "destroy" operation; typically a tree would be freed by
 * resetting or deleting the memory context it's stored in.  You can pfree
 * the RBTree node if you feel the urge.
 */
void
rb_create(RBTree *tree,
		  size_t node_size,
		  rb_comparator comparator,
		  rb_combiner combiner,
		  rb_allocfunc allocfunc,
		  rb_freefunc freefunc,
		  void *arg)
{
	assert(node_size > sizeof(RBNode));

	tree->root = RBNIL;
	tree->node_size = node_size;
	tree->comparator = comparator;
	tree->combiner = combiner;
	tree->allocfunc = allocfunc;
	tree->freefunc = freefunc;

	tree->arg = arg;
}

/* Copy the additional data fields from one RBNode to another */
static inline void
rb_copy_data(RBTree *rb, RBNode *dest, const RBNode *src)
{
	memcpy(dest + 1, src + 1, rb->node_size - sizeof(RBNode));
}

/**********************************************************************
 *						  Search									  *
 **********************************************************************/

/*
 * rb_find: search for a value in an RBTree
 *
 * data represents the value to try to find.  Its RBNode fields need not
 * be valid, it's the extra data in the larger struct that is of interest.
 *
 * Returns the matching tree entry, or NULL if no match is found.
 */
RBNode *
rb_find(RBTree *rb, const RBNode *data)
{
	RBNode	   *node = rb->root;

	while (node != RBNIL)
	{
		int			cmp = rb->comparator(data, node, rb->arg);

		if (cmp == 0)
			return node;
		else if (cmp < 0)
			node = node->left;
		else
			node = node->right;
	}

	return NULL;
}

/*
 * rb_leftmost: fetch the leftmost (smallest-valued) tree node.
 * Returns NULL if tree is empty.
 *
 * Note: in the original implementation this included an unlink step, but
 * that's a bit awkward.  Just call rb_delete on the result if that's what
 * you want.
 */
RBNode *
rb_leftmost(RBTree *rb)
{
	RBNode	   *node = rb->root;
	RBNode	   *leftmost = rb->root;

	while (node != RBNIL)
	{
		leftmost = node;
		node = node->left;
	}

	if (leftmost != RBNIL)
		return leftmost;

	return NULL;
}

/*
 * rb_rightmost: fetch the rightmost (highest-valued) tree node.
 * Returns NULL if tree is empty.
 */
RBNode *
rb_rightmost(RBTree *rb)
{
	RBNode	   *node = rb->root;
	RBNode	   *rightmost = rb->root;

	while (node != RBNIL)
	{
		rightmost = node;
		node = node->right;
	}

	if (rightmost != RBNIL)
		return rightmost;

	return NULL;
}

/**********************************************************************
 *							  Insertion								  *
 **********************************************************************/

/*
 * Rotate node x to left.
 *
 * x's right child takes its place in the tree, and x becomes the left
 * child of that node.
 */
static void
rb_rotate_left(RBTree *rb, RBNode *x)
{
	RBNode	   *y = x->right;

	/* establish x->right link */
	x->right = y->left;
	if (y->left != RBNIL)
		y->left->parent = x;

	/* establish y->parent link */
	if (y != RBNIL)
		y->parent = x->parent;
	if (x->parent)
	{
		if (x == x->parent->left)
			x->parent->left = y;
		else
			x->parent->right = y;
	}
	else
	{
		rb->root = y;
	}

	/* link x and y */
	y->left = x;
	if (x != RBNIL)
		x->parent = y;
}

/*
 * Rotate node x to right.
 *
 * x's left right child takes its place in the tree, and x becomes the right
 * child of that node.
 */
static void
rb_rotate_right(RBTree *rb, RBNode *x)
{
	RBNode	   *y = x->left;

	/* establish x->left link */
	x->left = y->right;
	if (y->right != RBNIL)
		y->right->parent = x;

	/* establish y->parent link */
	if (y != RBNIL)
		y->parent = x->parent;
	if (x->parent)
	{
		if (x == x->parent->right)
			x->parent->right = y;
		else
			x->parent->left = y;
	}
	else
	{
		rb->root = y;
	}

	/* link x and y */
	y->right = x;
	if (x != RBNIL)
		x->parent = y;
}

/*
 * Maintain Red-Black tree balance after inserting node x.
 *
 * The newly inserted node is always initially marked red.  That may lead to
 * a situation where a red node has a red child, which is prohibited.  We can
 * always fix the problem by a series of color changes and/or "rotations",
 * which move the problem progressively higher up in the tree.  If one of the
 * two red nodes is the root, we can always fix the problem by changing the
 * root from red to black.
 *
 * (This does not work lower down in the tree because we must also maintain
 * the invariant that every leaf has equal black-height.)
 */
static void
rb_insert_fixup(RBTree *rb, RBNode *x)
{
	/*
	 * x is always a red node.  Initially, it is the newly inserted node. Each
	 * iteration of this loop moves it higher up in the tree.
	 */
	while (x != rb->root && x->parent->color == RBRED)
	{
		/*
		 * x and x->parent are both red.  Fix depends on whether x->parent is
		 * a left or right child.  In either case, we define y to be the
		 * "uncle" of x, that is, the other child of x's grandparent.
		 *
		 * If the uncle is red, we flip the grandparent to red and its two
		 * children to black.  Then we loop around again to check whether the
		 * grandparent still has a problem.
		 *
		 * If the uncle is black, we will perform one or two "rotations" to
		 * balance the tree.  Either x or x->parent will take the
		 * grandparent's position in the tree and recolored black, and the
		 * original grandparent will be recolored red and become a child of
		 * that node. This always leaves us with a valid red-black tree, so
		 * the loop will terminate.
		 */
		if (x->parent == x->parent->parent->left)
		{
			RBNode	   *y = x->parent->parent->right;

			if (y->color == RBRED)
			{
				/* uncle is RBRED */
				x->parent->color = RBBLACK;
				y->color = RBBLACK;
				x->parent->parent->color = RBRED;

				x = x->parent->parent;
			}
			else
			{
				/* uncle is RBBLACK */
				if (x == x->parent->right)
				{
					/* make x a left child */
					x = x->parent;
					rb_rotate_left(rb, x);
				}

				/* recolor and rotate */
				x->parent->color = RBBLACK;
				x->parent->parent->color = RBRED;

				rb_rotate_right(rb, x->parent->parent);
			}
		}
		else
		{
			/* mirror image of above code */
			RBNode	   *y = x->parent->parent->left;

			if (y->color == RBRED)
			{
				/* uncle is RBRED */
				x->parent->color = RBBLACK;
				y->color = RBBLACK;
				x->parent->parent->color = RBRED;

				x = x->parent->parent;
			}
			else
			{
				/* uncle is RBBLACK */
				if (x == x->parent->left)
				{
					x = x->parent;
					rb_rotate_right(rb, x);
				}
				x->parent->color = RBBLACK;
				x->parent->parent->color = RBRED;

				rb_rotate_left(rb, x->parent->parent);
			}
		}
	}

	/*
	 * The root may already have been black; if not, the black-height of every
	 * node in the tree increases by one.
	 */
	rb->root->color = RBBLACK;
}

/*
 * rb_insert: insert a new value into the tree.
 *
 * data represents the value to insert.  Its RBNode fields need not
 * be valid, it's the extra data in the larger struct that is of interest.
 *
 * If the value represented by "data" is not present in the tree, then
 * we copy "data" into a new tree entry and return that node, setting *isNew
 * to true.
 *
 * If the value represented by "data" is already present, then we call the
 * combiner function to merge data into the existing node, and return the
 * existing node, setting *isNew to false.
 *
 * "data" is unmodified in either case; it's typically just a local
 * variable in the caller.
 */
RBNode *
rb_insert(RBTree *rb, const RBNode *data, bool *isNew)
{
	RBNode	   *current,
			   *parent,
			   *x;
	int			cmp;

	/* find where node belongs */
	current = rb->root;
	parent = NULL;
	cmp = 0;					/* just to prevent compiler warning */

	while (current != RBNIL)
	{
		cmp = rb->comparator(data, current, rb->arg);
		if (cmp == 0)
		{
			/*
			 * Found node with given key.  Apply combiner.
			 */
			rb->combiner(current, data, rb->arg);
			*isNew = false;
			return current;
		}
		parent = current;
		current = (cmp < 0) ? current->left : current->right;
	}

	/*
	 * Value is not present, so create a new node containing data.
	 */
	*isNew = true;

	x = rb->allocfunc (rb->arg);
	if(x == NULL)
		return NULL;

	x->color = RBRED;

	x->left = RBNIL;
	x->right = RBNIL;
	x->parent = parent;
	rb_copy_data(rb, x, data);

	/* insert node in tree */
	if (parent)
	{
		if (cmp < 0)
			parent->left = x;
		else
			parent->right = x;
	}
	else
	{
		rb->root = x;
	}

	rb_insert_fixup(rb, x);

	return x;
}

/**********************************************************************
 *							Deletion								  *
 **********************************************************************/

/*
 * Maintain Red-Black tree balance after deleting a black node.
 */
static void
rb_delete_fixup(RBTree *rb, RBNode *x)
{
	/*
	 * x is always a black node.  Initially, it is the former child of the
	 * deleted node.  Each iteration of this loop moves it higher up in the
	 * tree.
	 */
	while (x != rb->root && x->color == RBBLACK)
	{
		/*
		 * Left and right cases are symmetric.  Any nodes that are children of
		 * x have a black-height one less than the remainder of the nodes in
		 * the tree.  We rotate and recolor nodes to move the problem up the
		 * tree: at some stage we'll either fix the problem, or reach the root
		 * (where the black-height is allowed to decrease).
		 */
		if (x == x->parent->left)
		{
			RBNode	   *w = x->parent->right;

			if (w->color == RBRED)
			{
				w->color = RBBLACK;
				x->parent->color = RBRED;

				rb_rotate_left(rb, x->parent);
				w = x->parent->right;
			}

			if (w->left->color == RBBLACK && w->right->color == RBBLACK)
			{
				w->color = RBRED;

				x = x->parent;
			}
			else
			{
				if (w->right->color == RBBLACK)
				{
					w->left->color = RBBLACK;
					w->color = RBRED;

					rb_rotate_right(rb, w);
					w = x->parent->right;
				}
				w->color = x->parent->color;
				x->parent->color = RBBLACK;
				w->right->color = RBBLACK;

				rb_rotate_left(rb, x->parent);
				x = rb->root;	/* Arrange for loop to terminate. */
			}
		}
		else
		{
			RBNode	   *w = x->parent->left;

			if (w->color == RBRED)
			{
				w->color = RBBLACK;
				x->parent->color = RBRED;

				rb_rotate_right(rb, x->parent);
				w = x->parent->left;
			}

			if (w->right->color == RBBLACK && w->left->color == RBBLACK)
			{
				w->color = RBRED;

				x = x->parent;
			}
			else
			{
				if (w->left->color == RBBLACK)
				{
					w->right->color = RBBLACK;
					w->color = RBRED;

					rb_rotate_left(rb, w);
					w = x->parent->left;
				}
				w->color = x->parent->color;
				x->parent->color = RBBLACK;
				w->left->color = RBBLACK;

				rb_rotate_right(rb, x->parent);
				x = rb->root;	/* Arrange for loop to terminate. */
			}
		}
	}
	x->color = RBBLACK;
}

/*
 * Delete node z from tree.
 */
static void
rb_delete_node(RBTree *rb, RBNode *z)
{
	RBNode	   *x,
			   *y;

	if (!z || z == RBNIL)
		return;

	/*
	 * y is the node that will actually be removed from the tree.  This will
	 * be z if z has fewer than two children, or the tree successor of z
	 * otherwise.
	 */
	if (z->left == RBNIL || z->right == RBNIL)
	{
		/* y has a RBNIL node as a child */
		y = z;
	}
	else
	{
		/* find tree successor */
		y = z->right;
		while (y->left != RBNIL)
			y = y->left;
	}

	/* x is y's only child */
	if (y->left != RBNIL)
		x = y->left;
	else
		x = y->right;

	/* Remove y from the tree. */
	x->parent = y->parent;
	if (y->parent)
	{
		if (y == y->parent->left)
			y->parent->left = x;
		else
			y->parent->right = x;
	}
	else
	{
		rb->root = x;
	}

	/*
	 * If we removed the tree successor of z rather than z itself, then move
	 * the data for the removed node to the one we were supposed to remove.
	 */
	if (y != z)
		rb_copy_data(rb, z, y);

	/*
	 * Removing a black node might make some paths from root to leaf contain
	 * fewer black nodes than others, or it might make two red nodes adjacent.
	 */
	if (y->color == RBBLACK)
		rb_delete_fixup(rb, x);

	/* Now we can recycle the y node */
	if (rb->freefunc)
		rb->freefunc (y, rb->arg);
}

/*
 * rb_delete: remove the given tree entry
 *
 * "node" must have previously been found via rb_find or rb_leftmost.
 * It is caller's responsibility to free any subsidiary data attached
 * to the node before calling rb_delete.  (Do *not* try to push that
 * responsibility off to the freefunc, as some other physical node
 * may be the one actually freed!)
 */
void
rb_delete(RBTree *rb, RBNode *node)
{
	rb_delete_node(rb, node);
}

/**********************************************************************
 *						  Traverse									  *
 **********************************************************************/

/*
 * Begin left right walk.
 */
void
rb_begin_left_right_walk(RBTree *rb, RBTreeLeftRightWalk* lrw)
{
	lrw->rb = rb;
	lrw->last_visited = NULL;
	lrw->is_over = (lrw->rb->root == RBNIL);
}

/*
 * Left right walk: get next node. Returns NULL if there is none.
 */
RBNode*
rb_left_right_walk(RBTreeLeftRightWalk* lrw)
{
    if(lrw->is_over)
        return NULL;

    if(lrw->last_visited == NULL)
	{
        lrw->last_visited = lrw->rb->root;
		while(lrw->last_visited->left != RBNIL)
			lrw->last_visited = lrw->last_visited->left;

		return lrw->last_visited;
	}

	if(lrw->last_visited->right != RBNIL)
	{
		lrw->last_visited = lrw->last_visited->right;
		while(lrw->last_visited->left != RBNIL)
			lrw->last_visited = lrw->last_visited->left;

		return lrw->last_visited;
	}

	for(;;)
	{
		RBNode* came_from = lrw->last_visited;
		lrw->last_visited = lrw->last_visited->parent;
		if(lrw->last_visited == NULL)
		{
			lrw->is_over = true;
			break;
		}

		if(lrw->last_visited->left == came_from)
			break; /* came from left sub-tree, return current node */

		/* else - came from right sub-tree, continue to move up */
	}

    return lrw->last_visited;
}

/*
 * Begin right left walk.
 */
void
rb_begin_right_left_walk(RBTree *rb, RBTreeRightLeftWalk* rlw)
{
	rlw->rb = rb;
	rlw->last_visited = NULL;
	rlw->is_over = (rlw->rb->root == RBNIL);
}

/*
 * Right left walk: get next node. Returns NULL if there is none.
 */
RBNode*
rb_right_left_walk(RBTreeRightLeftWalk* rlw)
{
    if(rlw->is_over)
        return NULL;

    if(rlw->last_visited == NULL)
	{
        rlw->last_visited = rlw->rb->root;
		while(rlw->last_visited->right != RBNIL)
			rlw->last_visited = rlw->last_visited->right;

		return rlw->last_visited;
	}

	if(rlw->last_visited->left != RBNIL)
	{
		rlw->last_visited = rlw->last_visited->left;
		while(rlw->last_visited->right != RBNIL)
			rlw->last_visited = rlw->last_visited->right;

		return rlw->last_visited;
	}

	for(;;)
	{
		RBNode* came_from = rlw->last_visited;
		rlw->last_visited = rlw->last_visited->parent;
		if(rlw->last_visited == NULL)
		{
			rlw->is_over = true;
			break;
		}

		if(rlw->last_visited->right == came_from)
			break; /* came from right sub-tree, return current node */

		/* else - came from left sub-tree, continue to move up */
	}

    return rlw->last_visited;
}

/*
 * Begin direct walk (a.k.a pre-order traversal).
 *
 * Pre-order traversal while duplicating nodes and edges can make a complete
 * duplicate of a binary tree. It can also be used to make a prefix expression
 * (Polish notation) from expression trees: traverse the expression tree
 * pre-orderly.
 *
 * https://en.wikipedia.org/wiki/Tree_traversal#Applications
 */
void
rb_begin_direct_walk(RBTree *rb, RBTreeDirectWalk* dw)
{
	dw->rb = rb;
	dw->last_visited = NULL;
	dw->is_over = (dw->rb->root == RBNIL);
}

/*
 * Direct walk: get next node. Returns NULL if there is none.
 */
RBNode*
rb_direct_walk(RBTreeDirectWalk* dw)
{
    if(dw->is_over)
        return NULL;

    if(dw->last_visited == NULL)
    {
        dw->last_visited = dw->rb->root;
        return dw->last_visited;
    }

    if(dw->last_visited->left != RBNIL)
    {
        dw->last_visited = dw->last_visited->left;
        return dw->last_visited;
    }

    do
    {
        if(dw->last_visited->right != RBNIL)
        {
            dw->last_visited = dw->last_visited->right;
			break;
        }

		/* go up and one step right */
		for(;;)
		{
			RBNode* came_from = dw->last_visited;
        	dw->last_visited = dw->last_visited->parent;
			if(dw->last_visited == NULL)
			{
				dw->is_over = true;
				break;
			}

			if((dw->last_visited->right != came_from) && (dw->last_visited->right != RBNIL))
			{
				dw->last_visited = dw->last_visited->right;
				return dw->last_visited;
			}
		}
    }
	while(dw->last_visited != NULL);

    return dw->last_visited;
}

/*
 * Begin inverted walk (a.k.a post-order traversal).
 */
void
rb_begin_inverted_walk(RBTree *rb, RBTreeInvertedWalk* iw)
{
	iw->rb = rb;
	iw->last_visited = NULL;
	iw->next_step = NextStepNone;
	iw->is_over = (iw->rb->root == RBNIL);
}

/*
 * Inverted walk: get next node. Returns NULL if there is none.
 *
 * Post-order traversal while deleting or freeing nodes and values can delete
 * or free an entire binary tree. It can also generate a postfix
 * representation of a binary tree.
 *
 * https://en.wikipedia.org/wiki/Tree_traversal#Applications
 */
RBNode*
rb_inverted_walk(RBTreeInvertedWalk* iw)
{
	RBNode* came_from;

    if(iw->is_over)
        return NULL;

    if(iw->last_visited == NULL)
    {
        iw->last_visited = iw->rb->root;
		iw->next_step = NextStepLeft;
    }

loop:
	switch(iw->next_step)
	{
		case NextStepLeft:
			came_from = iw->last_visited;
			while(iw->last_visited->left != RBNIL)
				iw->last_visited = iw->last_visited->left;

			iw->next_step = NextStepRight;
			goto loop;

		case NextStepRight:
			if(iw->last_visited->right != RBNIL)
			{
				iw->last_visited = iw->last_visited->right;
				iw->next_step = NextStepLeft;
				goto loop;
			}
			else /* not moved - return current, then go up */
				iw->next_step = NextStepUp;
			break;
		case NextStepUp:
			for(;;)
			{
				came_from = iw->last_visited;
				iw->last_visited = iw->last_visited->parent;
				if(iw->last_visited == NULL)
				{
					iw->is_over = true;
					break; /* end of iteration */
				}

				if(came_from == iw->last_visited->right)
				{
					/* return current, then continue to go up */
					break;
				}

				/* otherwise we came from the left */
				iw->next_step = NextStepRight;
				goto loop;
			}
			break;
		default:
			fprintf(stderr, "Unexpected next step value during inverted walk: %d\n", iw->next_step);
			exit(1);
	}

	return iw->last_visited;
}

/*
 * Print tree to stdout.
 */
void
rb_tree_debug_print(RBTree* rb, rb_sprintfunc sprintfunc)
{
	RBTreeDirectWalk dw;
	RBNode* node;
	char buff[1024];

	rb_begin_direct_walk(rb, &dw);
	printf("nodes = {}\n");
	for(;;)
	{
		node = rb_direct_walk(&dw);
		if(!node)
			break;
		sprintfunc(node, buff);
		printf("nodes['%p'] = {'parent':'%p','left':'%p','right':'%p','color':'%s','data':'%s'}\n",
			(void*)node, (void*)node->parent, (void*)(node->left == RBNIL ? NULL : node->left), (void*)(node->right == RBNIL ? NULL : node->right),
			node->color == RBBLACK ? "black" : "red", buff);
	}
}

