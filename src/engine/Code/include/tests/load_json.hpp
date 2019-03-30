
// ---------------------------------------------------------------------------

Poco::JSON::Object::Ptr load_json( const std::string& _fname )
{
    std::ifstream input( _fname );

    std::stringstream json;

    std::string line;

    if ( input.is_open() )
        {
            while ( std::getline( input, line ) )
                {
                    json << line;
                }

            input.close();
        }
    else
        {
            throw std::invalid_argument( "File '" + _fname + "' not found!" );
        }

    return Poco::JSON::Parser()
        .parse( json.str() )
        .extract<Poco::JSON::Object::Ptr>();
}

// -----------------------------------------------------------------------------
