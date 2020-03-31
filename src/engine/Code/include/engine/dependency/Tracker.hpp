#ifndef ENGINE_DEPENDENCY_TRACKER_HPP_
#define ENGINE_DEPENDENCY_TRACKER_HPP_

namespace engine
{
namespace dependency
{
// -------------------------------------------------------------------------

template <class T>
class Tracker
{
   public:
    Tracker() {}

    ~Tracker() = default;

   public:
    /// Adds a new element to be tracked.
    void add( std::shared_ptr<const T> _elem );

    /// Removes all elements.
    void clear();

    /// Retrieves a deep copy of an element from the tracker, if an element
    /// containing this fingerprint exists.
    std::shared_ptr<T> retrieve(
        const Poco::JSON::Object::Ptr _fingerprint ) const;

   private:
    /// A map keeping track of the elements.
    std::map<size_t, std::shared_ptr<const T>> elements_;
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
void Tracker<T>::add( std::shared_ptr<const T> _elem )
{
    assert_true( _elem );

    const auto fingerprint = _elem->fingerprint();

    assert_true( fingerprint );

    const auto f_str = JSON::stringify( *fingerprint );

    const auto f_hash = std::hash<std::string>()( f_str );

    elements_.insert_or_assign( f_hash, _elem );
}

// -------------------------------------------------------------------------

template <class T>
void Tracker<T>::clear()
{
    *this = Tracker();
}

// -------------------------------------------------------------------------

template <class T>
std::shared_ptr<T> Tracker<T>::retrieve(
    const Poco::JSON::Object::Ptr _fingerprint ) const
{
    assert_true( _fingerprint );

    const auto f_str = JSON::stringify( *_fingerprint );

    const auto f_hash = std::hash<std::string>()( f_str );

    const auto it = elements_.find( f_hash );

    if ( it == elements_.end() )
        {
            return std::shared_ptr<T>();
        }

    const auto fingerprint2 = it->second->fingerprint();

    assert_true( fingerprint2 );

    const auto f2_str = JSON::stringify( *fingerprint2 );

    /// On the off-chance that there was a collision, we double-check.
    if ( f_str != f2_str )
        {
            return std::shared_ptr<T>();
        }

    return it->second->clone();
}

// -------------------------------------------------------------------------
}  // namespace dependency
}  // namespace engine

#endif  // ENGINE_DEPENDENCY_TRACKER_HPP_

