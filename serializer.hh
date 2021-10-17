#ifndef _SERIALIZER

#define _SERIALIZER

#include <cstdint>
#include <seastar/rpc/rpc.hh>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>

#include <rpc-proto.hh>

using namespace seastar;

struct serializer {};

template <typename T, typename Output>
inline void write_boost_serializable_type(Output &out, T v) {
  std::string serial_str;
  boost::iostreams::back_insert_device<std::string> inserter(serial_str);
  boost::iostreams::stream<boost::iostreams::back_insert_device<std::string>> s(
      inserter);
  boost::archive::binary_oarchive oa(s);

  oa << v;

  s.flush();

  intptr_t size = serial_str.size();

  out.write(reinterpret_cast<char *>(&size), sizeof(size));
  out.write(serial_str.data(), size);
}

template <typename T, typename Input>
inline T read_boost_serializable_type(Input &in) {
  intptr_t size;
  std::string serial_str;
  T obj;

  in.read(reinterpret_cast<char *>(&size), sizeof(size));
  serial_str.resize(size + 1);
  in.read(serial_str.data(), size);
  boost::iostreams::basic_array_source<char> device(serial_str.data(),
                                                    serial_str.size());
  boost::iostreams::stream<boost::iostreams::basic_array_source<char>> s(
      device);
  boost::archive::binary_iarchive ia(s);
  ia >> obj;

  return obj;
}

template <typename Output, typename T>
inline void write(serializer, Output &output, T v) {
  return write_boost_serializable_type(output, v);
}
template <typename Input, typename T>
inline T read(serializer, Input &input, rpc::type<T>) {
  return read_boost_serializable_type<T>(input);
}

#endif
