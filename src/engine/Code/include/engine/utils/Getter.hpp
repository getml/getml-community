#ifndef ENGINE_UTILS_GETTER_HPP_
#define ENGINE_UTILS_GETTER_HPP_

namespace engine
{
namespace utils
{
// ------------------------------------------------------------------------

struct Getter
{
    // ------------------------------------------------------------------------

    /// Gets object _name from map
    template <class T>
    static T &get( const std::string &_name, std::map<std::string, T> *_map );

    /// Gets object _name from map
    template <class T>
    static T get(
        const std::string &_name, const std::map<std::string, T> &_map );

    /// Gets vector of objects with _names from map
    template <class T>
    static std::vector<T> get(
        const std::vector<std::string> &_names,
        std::map<std::string, T> *&_map );

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <class T>
T &Getter::get( const std::string &_name, std::map<std::string, T> *_map )
{
    auto it = _map->find( _name );

    if ( it == _map->end() )
        {
            std::string warning_message = "'";
            warning_message.append( _name );
            warning_message.append( "' not found. " );
            warning_message.append( "Did you maybe forget to call .send()?" );

            throw std::invalid_argument( warning_message );
        }

    return it->second;
}

// ------------------------------------------------------------------------

template <class T>
T Getter::get( const std::string &_name, const std::map<std::string, T> &_map )
{
    auto it = _map.find( _name );

    if ( it == _map.end() )
        {
            std::string warning_message = "'";
            warning_message.append( _name );
            warning_message.append( "' not found. " );
            warning_message.append( "Did you maybe forget to call .send()?" );

            throw std::invalid_argument( warning_message );
        }

    return it->second;
}

// ------------------------------------------------------------------------

template <class T>
std::vector<T> Getter::get(
    const std::vector<std::string> &_names, std::map<std::string, T> *&_map )
{
    std::vector<T> vec;

    for ( auto &name : _names )
        {
            auto elem = get( _map, name );

            vec.push_back( elem );
        }

    return vec;
}

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_GETTER_HPP_
