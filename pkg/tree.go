package mk

type keyType []byte
type valueType []byte
type pair struct {
	key   keyType
	value valueType
}

// tree is the in-memory b+tree index
type tree struct {
	// order is the maxium number of keys in one node, the minium number of keys is order/2
	order int

	// pointer to root node
	root *node
}

type inode pair

type node struct {
	isLeaf   bool
	parent   *node
	inodes   []inode
	children []*node
}

func newTree() *tree {

}

// set returns (found, oldValue)
func (t *tree) set(key keyType, value valueType) (bool, valueType) {
	return t.root.set(key, value)
}

// get returns (found, value)
func (t *tree) get(key keyType) (bool, valueType) {
	return t.root.get(key)
}

// remove returns (found, oldValue)
func (t *tree) remove(key keyType) (bool, valueType) {

}

// print prints the tree to stdout
func (t *tree) print() {

}

func (n *node) set(key keyType, value valueType) (bool, valueType) {

}

func (n *node) get(key keyType) (bool, valueType) {

}

func (n *node) remove(key keyType) (bool, valueType) {

}

func (n *node) print() {

}

//
func (n *node) search(key keyType) int {

}
