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
    /// Returns the column names of the query.
    virtual std::vector<std::string> colnames() const = 0;

    /// Whether the end is reached.
    virtual bool end() const = 0;

    /// Returns a double and increments the iterator.
    virtual Float get_double() = 0;

    /// Returns a double and increments the iterator.
    virtual Int get_int() = 0;

    /// Returns a string and increments the iterator.
    virtual std::string get_string() = 0;

    /// Returns a time stamp transformed to the number of days since epoch and
    /// increments the iterator.
    virtual Float get_time_stamp() = 0;

    // -------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_ITERATOR_HPP_
