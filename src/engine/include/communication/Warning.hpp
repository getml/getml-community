#ifndef COMMUNICATION_WARNING_HPP_
#define COMMUNICATION_WARNING_HPP_

#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"

namespace communication {

using Warning = fct::NamedTuple<fct::Field<"message_", std::string>,
                                fct::Field<"label_", std::string>,
                                fct::Field<"warning_type_", std::string>>;

}

#endif
