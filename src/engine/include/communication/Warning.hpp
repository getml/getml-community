#ifndef COMMUNICATION_WARNING_HPP_
#define COMMUNICATION_WARNING_HPP_

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>

namespace communication {

using Warning = rfl::NamedTuple<rfl::Field<"message_", std::string>,
                                rfl::Field<"label_", std::string>,
                                rfl::Field<"warning_type_", std::string>>;

}

#endif
