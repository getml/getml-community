#ifndef MEMMAP_BTREE_HPP_
#define MEMMAP_BTREE_HPP_

namespace memmap
{
// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
class BTree
{
   private:
    using ChildPair =
        std::pair<BTreeNode<KeyType, ValueType>, BTreeNode<KeyType, ValueType>>;
    using KeyValuePair = std::pair<KeyType, ValueType>;
    using SplitReturnPair = std::pair<KeyValuePair, ChildPair>;

   public:
    /// Standard constructor.
    BTree( const std::shared_ptr<Pool> &_pool );

    /// Move constructor
    BTree( BTree<KeyType, ValueType> &&_other ) noexcept
        : order_( make_order() )
    {
        move( &_other );
    }

    /// Copy construtor.
    BTree( const BTree<KeyType, ValueType> &_other ) = delete;

    ~BTree() { deallocate(); }

   public:
    /// Inserts a new key-value-pair into the tree
    void insert( const KeyType _key, const ValueType _value );

    /// Returns a vector containing all keys (deep copy)
    Vector<KeyType> keys() const;

    /// Move assignment operator.
    BTree<KeyType, ValueType> &operator=(
        BTree<KeyType, ValueType> &&_other ) noexcept;

    /// Copy assignment operator.
    BTree<KeyType, ValueType> &operator=(
        const BTree<KeyType, ValueType> &_other ) = delete;

    /// Returns a vector containing all values (deep copy)
    Vector<ValueType> values() const;

   public:
    /// Whether the root of the BTree is allocated.
    bool is_allocated() const { return root_.is_allocated(); }

    /// Access operator
    std::optional<ValueType> operator[]( KeyType _key ) const
    {
        return get_value( _key, root_ );
    }

    /// Returns the size of the BTree
    size_t size() const { return root_.size(); }

   private:
    /// Creates a new node.
    static BTreeNode<KeyType, ValueType> allocate_new_node(
        const std::shared_ptr<Pool> &_pool );

    /// Returns the position of the first element in _keys that isn't smaller
    /// than _key or _keys.size() if there is none.
    size_t find_pos(
        const KeyType _key, const VectorImpl<KeyType> &_keys ) const;

    /// Iteratively searches through the node to return the value.
    std::optional<ValueType> get_value(
        const KeyType _key, const BTreeNode<KeyType, ValueType> &_node ) const;

    /// Inserts a new child node pair into a node,
    /// assuming we already now that it is the correct node
    /// and the desired position and have already added the corresponding
    /// key-value-pair.
    void insert_children(
        const BTreeNode<KeyType, ValueType> &_smaller,
        const BTreeNode<KeyType, ValueType> &_larger,
        const size_t _pos,
        BTreeNode<KeyType, ValueType> *_node ) const;

    /// Recurses through the tree to find the right node to insert. That node
    /// will be split, if necessary.
    std::optional<SplitReturnPair> insert_into_tree(
        const KeyType _key,
        const ValueType _value,
        BTreeNode<KeyType, ValueType> *_node ) const;

    /// Generates a vector containing all keys (deep copy)
    void insert_keys(
        const BTreeNode<KeyType, ValueType> _node,
        Vector<KeyType> *_vec ) const;

    /// Generates a vector containing all values (deep copy)
    void insert_values(
        const BTreeNode<KeyType, ValueType> _node,
        Vector<ValueType> *_vec ) const;

    /// Inserts a new key-value-pair into a node,
    /// assuming we already now that it is the correct node
    /// and the desired position
    void insert_key_value(
        const KeyType _key,
        const ValueType _value,
        const size_t _pos,
        BTreeNode<KeyType, ValueType> *_node ) const;

    /// Implements the move operation.
    void move( BTree<KeyType, ValueType> *_other );

    /// Splits a node into two (almost) equally sized subnodes and a median
    /// key-value-pair.
    SplitReturnPair split_node( BTreeNode<KeyType, ValueType> *_node ) const;

    /// Returns the smaller part of the node to be split.
    BTreeNode<KeyType, ValueType> split_node_make_smaller(
        const size_t _median_pos, BTreeNode<KeyType, ValueType> *_node ) const;

    /// Returns the greater part of the node to be split.
    BTreeNode<KeyType, ValueType> split_node_make_greater(
        const BTreeNode<KeyType, ValueType> &_node,
        const size_t _median_pos ) const;

   private:
    /// Deallocates the resources
    void deallocate() { root_.deallocate(); }

    /// Initializes the order of the BTree.
    static size_t make_order()
    {
        return std::max(
            getpagesize() / sizeof( KeyType ) - 1,
            static_cast<unsigned long>( 16 ) );
    }

   private:
    /// The order of the BTree.
    const size_t order_;

    /// The pool containing the underlying data.
    std::shared_ptr<Pool> pool_;

    /// The root of the BTree.
    BTreeNode<KeyType, ValueType> root_;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
BTree<KeyType, ValueType>::BTree( const std::shared_ptr<Pool> &_pool )
    : order_( make_order() ),
      pool_( _pool ),
      root_( allocate_new_node( _pool ) )
{
    assert_true( pool_ );
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
BTreeNode<KeyType, ValueType> BTree<KeyType, ValueType>::allocate_new_node(
    const std::shared_ptr<Pool> &_pool )
{
    BTreeNode<KeyType, ValueType> new_node;

    new_node.keys_ = Vector<KeyType>( _pool ).yield_impl();

    new_node.values_ = Vector<ValueType>( _pool ).yield_impl();

    return new_node;
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
size_t BTree<KeyType, ValueType>::find_pos(
    const KeyType _key, const VectorImpl<KeyType> &_keys ) const
{
    for ( size_t i = 0; i < _keys.size(); ++i )
        {
            if ( _keys[i] >= _key )
                {
                    return i;
                }
        }

    return _keys.size();
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
void BTree<KeyType, ValueType>::insert_children(
    const BTreeNode<KeyType, ValueType> &_smaller,
    const BTreeNode<KeyType, ValueType> &_larger,
    const size_t _pos,
    BTreeNode<KeyType, ValueType> *_node ) const
{
    assert_true( _node->is_allocated() );

    if ( _node->is_leaf() )
        {
            // This means that is a new root node and we are in the process of
            // growing the tree.
            assert_true( _node->keys_.size() == 1 );
            assert_true( _node->values_.size() == 1 );

            assert_true( pool_ );

            _node->child_nodes_ =
                Vector<BTreeNode<KeyType, ValueType>>( pool_ ).yield_impl();

            _node->child_nodes_.push_back( _smaller );
            _node->child_nodes_.push_back( _larger );
        }
    else
        {
            assert_true( _pos < _node->child_nodes_.size() );

            _node->child_nodes_.insert( _pos, _smaller );

            // The node that used to be at this position must have been the node
            // that was split and then yielded.
            assert_true(
                !_node->child_nodes_[_pos + 1].keys_.is_allocated() &&
                !_node->child_nodes_[_pos + 1].values_.is_allocated() );

            _node->child_nodes_[_pos + 1] = _larger;
        }

    assert_true( _node->keys_.size() == _node->values_.size() );
    assert_true( _node->keys_.size() + 1 == _node->child_nodes_.size() );
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
void BTree<KeyType, ValueType>::insert(
    const KeyType _key, const ValueType _value )
{
    const auto split_return_pair = insert_into_tree( _key, _value, &root_ );

    if ( !split_return_pair ) [[likely]]
        {
            return;
        }

    // The root node must have been split and its ressources have been yielded.
    assert_true( !root_.keys_.is_allocated() && !root_.values_.is_allocated() );

    auto new_root_node = allocate_new_node( pool_ );

    insert_key_value(
        split_return_pair->first.first,
        split_return_pair->first.second,
        0,
        &new_root_node );

    insert_children(
        split_return_pair->second.first,
        split_return_pair->second.second,
        0,
        &new_root_node );

    root_ = new_root_node;
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
std::optional<typename BTree<KeyType, ValueType>::SplitReturnPair>
BTree<KeyType, ValueType>::insert_into_tree(
    const KeyType _key,
    const ValueType _value,
    BTreeNode<KeyType, ValueType> *_node ) const
{
    assert_true( _node->is_allocated() );

    const auto pos = find_pos( _key, _node->keys_ );

    if ( pos < _node->keys_.size() && _node->keys_[pos] == _key )
        {
            insert_key_value( _key, _value, pos, _node );
            return std::nullopt;
        }

    if ( _node->is_leaf() )
        {
            insert_key_value( _key, _value, pos, _node );

            if ( _node->keys_.size() > order_ )
                {
                    return split_node( _node );
                }

            return std::nullopt;
        }

    assert_true( _node->child_nodes_.is_allocated() );

    assert_true( pos < _node->child_nodes_.size() );

    assert_true( _node->child_nodes_[pos].is_allocated() );

    // It is necessary to copy the child node,
    // so that the pointer isn't invalidated, if
    // the pool is resized.
    auto child_node = _node->child_nodes_[pos];

    const auto split_return_pair =
        insert_into_tree( _key, _value, &child_node );

    _node->child_nodes_[pos] = child_node;

    if ( !split_return_pair ) [[likely]]
        {
            return std::nullopt;
        }

    assert_true( _node->is_allocated() );

    insert_key_value(
        split_return_pair->first.first,
        split_return_pair->first.second,
        pos,
        _node );

    insert_children(
        split_return_pair->second.first,
        split_return_pair->second.second,
        pos,
        _node );

    if ( _node->keys_.size() > order_ )
        {
            return split_node( _node );
        }

    return std::nullopt;
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
void BTree<KeyType, ValueType>::insert_key_value(
    const KeyType _key,
    const ValueType _value,
    const size_t _pos,
    BTreeNode<KeyType, ValueType> *_node ) const
{
    assert_true( _node->is_allocated() );
    assert_true( _node->keys_.size() == _node->values_.size() );
    assert_true( _pos <= _node->keys_.size() );

    if ( _pos == _node->keys_.size() )
        {
            _node->keys_.push_back( _key );
            _node->values_.push_back( _value );
            return;
        }

    if ( _node->keys_[_pos] == _key )
        {
            _node->values_[_pos] = _value;
            return;
        }

    assert_true( _node->keys_[_pos] > _key );

    _node->keys_.insert( _pos, _key );
    _node->values_.insert( _pos, _value );
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
void BTree<KeyType, ValueType>::insert_keys(
    const BTreeNode<KeyType, ValueType> _node, Vector<KeyType> *_vec ) const
{
    for ( const auto k : _node.keys_ )
        {
            _vec->push_back( k );
        }

    if ( !_node.is_leaf() )
        {
            for ( const auto child : _node.child_nodes_ )
                {
                    insert_keys( child, _vec );
                }
        }
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
void BTree<KeyType, ValueType>::insert_values(
    const BTreeNode<KeyType, ValueType> _node, Vector<ValueType> *_vec ) const
{
    for ( const auto v : _node.values_ )
        {
            _vec->push_back( v );
        }

    if ( !_node.is_leaf() )
        {
            for ( const auto child : _node.child_nodes_ )
                {
                    insert_values( child, _vec );
                }
        }
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
Vector<KeyType> BTree<KeyType, ValueType>::keys() const
{
    assert_true( pool_ );
    auto vec =
        Vector<KeyType>( std::make_shared<memmap::Pool>( pool_->temp_dir() ) );
    insert_keys( root_, &vec );
    return vec;
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
void BTree<KeyType, ValueType>::move( BTree<KeyType, ValueType> *_other )
{
    root_ = _other->root_.yield_ressources();
    pool_ = _other->pool_;
    assert_true( root_.is_allocated() );
    assert_true( pool_ );
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
std::optional<ValueType> BTree<KeyType, ValueType>::get_value(
    const KeyType _key, const BTreeNode<KeyType, ValueType> &_node ) const
{
    if ( _node.keys_.size() == 0 )
        {
            return std::nullopt;
        }

    assert_true( _node.keys_.size() == _node.values_.size() );

    assert_true(
        _node.is_leaf() ||
        _node.keys_.size() + 1 == _node.child_nodes_.size() );

    for ( size_t i = 0; i < _node.keys_.size(); ++i )
        {
            if ( _node.keys_[i] == _key )
                {
                    return _node.values_[i];
                }

            if ( _node.keys_[i] > _key )
                {
                    if ( _node.is_leaf() )
                        {
                            return std::nullopt;
                        }

                    return get_value( _key, _node.child_nodes_[i] );
                }
        }

    if ( _node.is_leaf() )
        {
            return std::nullopt;
        }

    return get_value( _key, _node.child_nodes_[_node.keys_.size()] );
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
BTree<KeyType, ValueType> &BTree<KeyType, ValueType>::operator=(
    BTree<KeyType, ValueType> &&_other ) noexcept
{
    if ( &_other == this )
        {
            return *this;
        }

    deallocate();

    move( &_other );

    return *this;
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
typename BTree<KeyType, ValueType>::SplitReturnPair
BTree<KeyType, ValueType>::split_node(
    BTreeNode<KeyType, ValueType> *_node ) const
{
    assert_true( _node->is_allocated() );

    assert_true( _node->keys_.size() == _node->values_.size() );

    assert_true(
        !_node->child_nodes_.is_allocated() ||
        _node->keys_.size() + 1 == _node->child_nodes_.size() );

    assert_true( _node->keys_.size() >= 3 );

    const auto median_pos = _node->keys_.size() / 2;

    const auto key_value_pair =
        std::make_pair( _node->keys_[median_pos], _node->values_[median_pos] );

    const auto node_greater = split_node_make_greater( *_node, median_pos );

    const auto node_smaller = split_node_make_smaller( median_pos, _node );

    const auto child_node_pair = std::make_pair( node_smaller, node_greater );

    return std::make_pair( key_value_pair, child_node_pair );
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
BTreeNode<KeyType, ValueType>
BTree<KeyType, ValueType>::split_node_make_smaller(
    const size_t _median_pos, BTreeNode<KeyType, ValueType> *_node ) const
{
    assert_true( _node->is_allocated() );

    assert_true( _node->keys_.size() == _node->values_.size() );

    const auto child_nodes = _node->child_nodes_.is_allocated()
                                 ? VectorImpl<BTreeNode<KeyType, ValueType>>(
                                       _node->child_nodes_.page_num(),
                                       _node->child_nodes_.pool(),
                                       _median_pos + 1 )
                                 : VectorImpl<BTreeNode<KeyType, ValueType>>();

    const auto keys = VectorImpl<KeyType>(
        _node->keys_.page_num(), _node->keys_.pool(), _median_pos );

    const auto values = VectorImpl<ValueType>(
        _node->values_.page_num(), _node->values_.pool(), _median_pos );

    _node->yield_ressources();

    return BTreeNode<KeyType, ValueType>{
        .child_nodes_ = child_nodes, .keys_ = keys, .values_ = values };
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
BTreeNode<KeyType, ValueType>
BTree<KeyType, ValueType>::split_node_make_greater(
    const BTreeNode<KeyType, ValueType> &_node, const size_t _median_pos ) const
{
    assert_true( _node.is_allocated() );

    auto child_nodes = VectorImpl<BTreeNode<KeyType, ValueType>>();

    if ( _node.child_nodes_.is_allocated() )
        {
            auto child_nodes_vec =
                Vector<BTreeNode<KeyType, ValueType>>( pool_ );

            for ( size_t i = _median_pos + 1; i < _node.child_nodes_.size();
                  ++i )
                {
                    child_nodes_vec.push_back( _node.child_nodes_[i] );
                }

            child_nodes = child_nodes_vec.yield_impl();
        }

    auto keys = Vector<KeyType>( pool_ );

    for ( size_t i = _median_pos + 1; i < _node.keys_.size(); ++i )
        {
            keys.push_back( _node.keys_[i] );
        }

    auto values = Vector<ValueType>( pool_ );

    for ( size_t i = _median_pos + 1; i < _node.values_.size(); ++i )
        {
            values.push_back( _node.values_[i] );
        }

    return BTreeNode<KeyType, ValueType>{
        .child_nodes_ = child_nodes,
        .keys_ = keys.yield_impl(),
        .values_ = values.yield_impl() };
}

// ----------------------------------------------------------------------------

template <class KeyType, class ValueType>
Vector<ValueType> BTree<KeyType, ValueType>::values() const
{
    assert_true( pool_ );
    auto vec = Vector<ValueType>(
        std::make_shared<memmap::Pool>( pool_->temp_dir() ) );
    insert_values( root_, &vec );
    return vec;
}

// ----------------------------------------------------------------------------
}  // namespace memmap

#endif  // MEMMAP_BTREE_HPP_
