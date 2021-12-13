#ifndef MEMMAP_BTREENODE_HPP_
#define MEMMAP_BTREENODE_HPP_

namespace memmap
{
// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
struct BTreeNode
{
    /// Dallocate the resources. Note that RAII
    /// does not work in a memory-mapped environment.
    /// Therefore, we have to take care of the deallocation
    /// ourselves.
    void deallocate();

    /// Whether the node is allocated.
    bool is_allocated() const
    {
        assert_true( keys_.is_allocated() == values_.is_allocated() );
        assert_true( keys_.is_allocated() || !child_nodes_.is_allocated() );
        return keys_.is_allocated();
    }

    /// Whether a node is a leaf. Because of the way a BTree works,
    /// either all nodes have child nodes or none of the nodes do.
    bool is_leaf() const { return !child_nodes_.is_allocated(); }

    /// Returns the size of the BTreeNode and all of its children.
    size_t size() const
    {
        assert_true( keys_.is_allocated() );
        assert_true( values_.is_allocated() );
        assert_true( keys_.size() == values_.size() );

        if ( is_leaf() )
            {
                return keys_.size();
            }

        const auto get_size =
            []( const size_t _init,
                const BTreeNode<KeyType, ValueType>& _node ) -> size_t {
            return _init + _node.size();
        };

        return keys_.size() +
               std::accumulate(
                   child_nodes_.begin(), child_nodes_.end(), 0, get_size );
    }

    /// Yields all ressources. If deallocate is called on this BTreeNode,
    /// then nothing will happen.
    /// The yielded BTreeNode will then be
    /// responsible for deallocation.
    BTreeNode<KeyType, ValueType> yield_ressources();

    /// Contains the children.
    VectorImpl<BTreeNode<KeyType, ValueType>> child_nodes_;

    /// A vector of keys.
    VectorImpl<KeyType> keys_;

    /// A vector of values.
    VectorImpl<ValueType> values_;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
void BTreeNode<KeyType, ValueType>::deallocate()
{
    if ( child_nodes_.size() > 0 )
        {
            for ( auto& child : child_nodes_ )
                {
                    child.deallocate();
                }
        }

    child_nodes_.deallocate();

    keys_.deallocate();

    values_.deallocate();
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
BTreeNode<KeyType, ValueType> BTreeNode<KeyType, ValueType>::yield_ressources()
{
    auto node = *this;

    // This prevents deallocate from doing anything.
    child_nodes_.yield_ressources();
    keys_.yield_ressources();
    values_.yield_ressources();

    return node;
}

// ----------------------------------------------------------------------------
}  // namespace memmap

#endif  // MEMMAP_BTREENODE_HPP_
