#ifndef _RPC_PROTO
#define _RPC_PROTO

#include <string>
#include <vector>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/optional.hpp>

#include "ycsbc.h"

struct RPCReadParam {
  std::string table;
  std::string key;
  boost::optional<std::vector<std::string>> fields;

  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar &table;
    ar &key;
    ar &fields;
  }
};

using RPCReadResult = std::tuple<int, std::vector<ycsbc::DB::KVPair>>;

struct RPCScanParam {
  std::string table;
  std::string key;
  int len;
  boost::optional<std::vector<std::string>> fields;

  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar &table;
    ar &key;
    ar &len;
    ar &fields;
  }
};

using RPCScanResult =
    std::tuple<int, std::vector<std::vector<ycsbc::DB::KVPair>>>;

struct RPCMultiReadParam {
  std::string table;
  std::vector<std::string> keys;
  boost::optional<std::vector<std::string>> fields;

  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar &table;
    ar &keys;
    ar &fields;
  }
};

using RPCMultiReadResult =
    std::tuple<int, std::vector<std::vector<ycsbc::DB::KVPair>>>;

struct RPCUpdateParam {
  std::string table;
  std::string key;
  std::vector<ycsbc::DB::KVPair> values;

  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar &table;
    ar &key;
    ar &values;
  }
};

using RPCUpdateResult = std::tuple<int>;

struct RPCInsertParam {
  std::string table;
  std::string key;
  std::vector<ycsbc::DB::KVPair> values;

  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar &table;
    ar &key;
    ar &values;
  }
};

using RPCInsertResult = std::tuple<int>;

struct RPCDeleteParam {
  std::string table;
  std::string key;

  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar &table;
    ar &key;
  }
};

using RPCDeleteResult = std::tuple<int>;

namespace boost {
namespace serialization {
template <typename Archive, typename... Types>
void serialize(Archive &ar, std::tuple<Types...> &t, const unsigned int) {
  std::apply([&](auto &...element) { ((ar & element), ...); }, t);
}
} // namespace serialization
} // namespace boost

#endif
