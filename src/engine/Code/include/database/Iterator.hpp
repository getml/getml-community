#ifndef DATABASE_ITERATOR_HPP_
#define DATABASE_ITERATOR_HPP_

namespace database
{
// ----------------------------------------------------------------------------

class Iterator
{
    // -------------------------------

   public:
    Iterator() {}

    virtual ~Iterator() = default;

    // -------------------------------

   public:
    /// Whether the end is reached.
    virtual bool end() const = 0;

    /// Returns a double and increments the iterator.
    virtual DATABASE_FLOAT get_double() = 0;

    /// Returns a double and increments the iterator.
    virtual DATABASE_INT get_int() = 0;

    /// Returns a string and increments the iterator.
    virtual std::string get_string() = 0;

    /// Returns a time stamp transformed to the number of days since epoch and
    /// increments the iterator.
    virtual DATABASE_FLOAT get_time_stamp() = 0;

    // -------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_ITERATOR_HPP_
