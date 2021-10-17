#include "ycsbc.h"

#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <typeinfo>

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/rpc/rpc.hh>

#include <serializer.hh>

using std::endl;

namespace bpo = boost::program_options;

struct {
  template <typename T> auto &operator<<(T &&) { return *this; }
} debug;

namespace ycsbc {

class BasicDB : public DB {
  inline static seastar::rpc::protocol<serializer> myrpc{serializer()};

  inline static auto rpcread =
      myrpc.make_client<RPCReadResult(RPCReadParam)>(1);
  inline static auto rpcscan =
      myrpc.make_client<RPCScanResult(RPCScanParam)>(2);
  inline static auto rpcmultiread =
      myrpc.make_client<RPCMultiReadResult(RPCMultiReadParam)>(3);
  inline static auto rpcupdate =
      myrpc.make_client<RPCUpdateResult(RPCUpdateParam)>(4);
  inline static auto rpcinsert =
      myrpc.make_client<RPCInsertResult(RPCInsertParam)>(5);
  inline static auto rpcdelete =
      myrpc.make_client<RPCDeleteResult(RPCDeleteParam)>(6);

  std::string server_addr;
  ushort port;

public:
  BasicDB(std::string server_addr, ushort port)
      : server_addr(server_addr), port(port) {}

  inline rpc::protocol<serializer>::client &getClient() {
    thread_local rpc::protocol<serializer>::client *pclient = nullptr;

    if (pclient == nullptr)
      pclient = new rpc::protocol<serializer>::client(
          myrpc, ipv4_addr{server_addr, port});

    return *pclient;
  }

  void Init() {
    std::lock_guard<std::mutex> lock(mutex_);

    debug << "A new thread begins working." << '\n';
  }

  seastar::future<int> Read(const std::string &table, const std::string &key,
                            const std::vector<std::string> *fields,
                            std::vector<KVPair> &result) {
    debug << "READ " << table << ' ' << key;
    if (fields) {
      debug << " [ ";
      for (auto f : *fields) {
        debug << f << ' ';
      }
      debug << ']' << '\n';
    } else {
      debug << " < all fields >" << '\n';
    }

    RPCReadParam param = {table, key, boost::none};

    if (fields)
      param.fields = *fields;

    auto [num, rpcresult] = rpcread(getClient(), param).get();

    result = rpcresult;

    return seastar::make_ready_future<int>(num);
  }

  seastar::future<int> Scan(const std::string &table, const std::string &key,
                            int len, const std::vector<std::string> *fields,
                            std::vector<std::vector<KVPair>> &result) {
    // std::lock_guard<std::mutex> lock(mutex_);
    debug << "SCAN " << table << ' ' << key << " " << len;
    if (fields) {
      debug << " [ ";
      for (auto f : *fields) {
        debug << f << ' ';
      }
      debug << ']' << '\n';
    } else {
      debug << " < all fields >" << '\n';
    }

    RPCScanParam param = {table, key, len, boost::none};

    if (fields)
      param.fields = *fields;

    auto [num, rpcresult] = rpcscan(getClient(), param).get();

    result = rpcresult;

    return seastar::make_ready_future<int>(num);
  }

  seastar::future<int> MultiRead(const std::string &table,
                                 const std::vector<std::string> &keys,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<KVPair>> &result) {
    // std::lock_guard<std::mutex> lock(mutex_);
    debug << "MultiGet " << table << ' ';
    debug << "keys:\t[ ";
    for (auto &key : keys) {
      debug << key << ' ';
    }
    debug << "]" << '\n';
    if (fields) {
      debug << "fields:\t[ ";
      // for (auto f : *fields) {
      //   debug << f << ' ';
      // }
      debug << ']' << '\n';
    } else {
      debug << " < all fields >" << '\n';
    }

    RPCMultiReadParam param = {table, keys, boost::none};

    if (fields)
      param.fields = *fields;

    auto [num, rpcresult] = rpcmultiread(getClient(), param).get();

    result = rpcresult;

    return seastar::make_ready_future<int>(num);
  }

  seastar::future<int> Update(const std::string &table, const std::string &key,
                              std::vector<KVPair> &values) {
    // std::lock_guard<std::mutex> lock(mutex_);
    debug << "UPDATE " << table << ' ' << key << " [ ";
    for (auto v : values) {
      debug << v.first << '=' << v.second << ' ';
    }
    debug << ']' << '\n';

    RPCUpdateParam param = {table, key, values};

    auto [num] = rpcupdate(getClient(), param).get();

    return seastar::make_ready_future<int>(num);
  }

  seastar::future<int> Insert(const std::string &table, const std::string &key,
                              std::vector<KVPair> &values) {
    // std::lock_guard<std::mutex> lock(mutex_);
    debug << "INSERT " << table << ' ' << key << " [ ";
    for (auto v : values) {
      debug << v.first << '=' << v.second << ' ';
    }
    debug << ']' << '\n';

    RPCInsertParam param = {table, key, values};

    auto [num] = rpcinsert(getClient(), param).get();

    return seastar::make_ready_future<int>(num);
  }

  seastar::future<int> Delete(const std::string &table,
                              const std::string &key) {
    // std::lock_guard<std::mutex> lock(mutex_);
    debug << "DELETE " << table << ' ' << key << '\n';

    RPCDeleteParam param = {table, key};

    auto [num] = rpcdelete(getClient(), param).get();

    return seastar::make_ready_future<int>(num);
  }

private:
  std::mutex mutex_;
};

} // namespace ycsbc

int main(int argc, char *argv[]) {

  using namespace std::string_literals;

  std::vector<char *> seastar_args, ycsbc_args;

  seastar_args.push_back(argv[0]);
  ycsbc_args.push_back(argv[0]);

  {
    bool sep_met = false;

    for (int i = 1; i < argc; i++) {
      if (argv[i] == "--"s) {
        sep_met = true;
      } else if (sep_met) {
        ycsbc_args.push_back(argv[i]);
      } else {
        seastar_args.push_back(argv[i]);
      }
    }

    if (!sep_met) {
      std::cerr << "Usage: " << argv[0] << " <seastar args> -- <ycsbc args>"
                << std::endl;
      return 1;
    }
  }

  seastar::app_template app;
  app.add_options()("address",
                    bpo::value<std::string>()->default_value("127.0.0.1"),
                    "Server address");
  app.add_options()("port", bpo::value<ushort>()->default_value(10000),
                    "Server port");

  app.run(seastar_args.size(), seastar_args.data(), [ycsbc_args, &app] {
    return seastar::async([ycsbc_args, config = app.configuration()]() mutable -> void {
      std::string address = config["address"].as<std::string>();
      ushort port = config["port"].as<ushort>();

      ycsbc::DB *basic_db = new ycsbc::BasicDB(address, port);
      ycsbc::RunBench(ycsbc_args.size(),
                      const_cast<const char **>(ycsbc_args.data()), basic_db);
    });
  });
}
