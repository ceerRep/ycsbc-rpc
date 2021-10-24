#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>

#include <sys/types.h>

#include <rpc-proto.hh>
#include <serializer.hh>

#include <xxhash.h>

#include <fmt/core.h>

using namespace std;
using namespace std::string_literals;
using namespace seastar;
namespace bpo = boost::program_options;

struct {
  template <typename T> auto &operator<<(T &&) { return *this; }
} debug;

class StdMapBackend {
  struct bucket {
    std::map<std::string, std::string> storage;
    seastar::shared_mutex mutex;
    seastar::semaphore sem{1};
  };

  int bucket_num;

public:
  StdMapBackend(int bucket_num) : bucket_num(bucket_num) {}

  bucket &getBucket() {
    thread_local static bucket b;
    return b;
  }

  seastar::future<std::string> get(const std::string &key) {
    XXH64_hash_t hash = XXH64(key.data(), key.size(), 0);
    int index = hash % bucket_num;

    seastar::foreign_ptr<std::unique_ptr<std::string>> fp_key(
        std::make_unique<std::string>(key));

    seastar::promise<std::string> *promise = new seastar::promise<std::string>;
    (void)seastar::smp::submit_to(
        index, [this, source_shard_id = seastar::this_shard_id(), index,
                fp_key = std::move(fp_key), promise]() mutable {
          auto &b = getBucket();
          return b.sem.wait(1)
              .then([this, source_shard_id, index, fp_key = std::move(fp_key),
                     promise, &b]() {
                seastar::foreign_ptr<std::unique_ptr<std::string>> fp_value(
                    std::make_unique<std::string>(b.storage[*fp_key]));
                return seastar::smp::submit_to(
                    source_shard_id,
                    [fp_value = std::move(fp_value), promise]() {
                      promise->set_value(std::string(*fp_value));
                    });
              })
              .finally([index, &b] { b.sem.signal(1); });
        });

    auto fut = promise->get_future();

    return fut.finally([promise]() { delete promise; });
  }

  seastar::future<> set(const std::string &key, const std::string &value) {
    XXH64_hash_t hash = XXH64(key.data(), key.size(), 0);
    int index = hash % bucket_num;

    seastar::foreign_ptr<std::unique_ptr<std::string>> fp_key(
        std::make_unique<std::string>(key));
    seastar::foreign_ptr<std::unique_ptr<std::string>> fp_value(
        std::make_unique<std::string>(value));

    seastar::promise<> *promise = new seastar::promise<>;
    (void)seastar::smp::submit_to(
        index, [this, source_shard_id = seastar::this_shard_id(), index,
                fp_key = std::move(fp_key), fp_value = std::move(fp_value),
                promise]() mutable {
          auto &b = getBucket();
          return b.sem.wait(1)
              .then([source_shard_id, index, fp_key = std::move(fp_key),
                     fp_value = std::move(fp_value), promise, &b]() {
                b.storage[*fp_key] = *fp_value;
                return seastar::smp::submit_to(
                    source_shard_id, [promise]() { promise->set_value(); });
              })
              .finally([index, &b] { b.sem.signal(1); });
        });

    auto fut = promise->get_future();

    return fut.finally([promise]() { delete promise; });
  }

  seastar::future<> remove(const std::string &key) {
    XXH64_hash_t hash = XXH64(key.data(), key.size(), 0);
    int index = hash % bucket_num;

    seastar::foreign_ptr<std::unique_ptr<std::string>> fp_key(
        std::make_unique<std::string>(key));

    seastar::promise<> *promise = new seastar::promise<>;
    (void)seastar::smp::submit_to(
        index, [this, source_shard_id = seastar::this_shard_id(), index,
                fp_key = std::move(fp_key), promise]() mutable {
          auto &b = getBucket();
          return b.sem.wait(1)
              .then([source_shard_id, index, fp_key = std::move(fp_key),
                     promise, &b]() {
                b.storage.erase(*fp_key);
                return seastar::smp::submit_to(
                    source_shard_id, [promise]() { promise->set_value(); });
              })
              .finally([index, &b] { b.sem.signal(1); });
        });

    auto fut = promise->get_future();

    return fut.finally([promise]() { delete promise; });
  }
};

class RPCServer {
  StdMapBackend backend;
  rpc::protocol<serializer> myrpc;

public:
  RPCServer(std::string address, ushort port)
      : backend(seastar::smp::count), myrpc(serializer{}) {
    myrpc.register_handler(
        1, [this](RPCReadParam param) -> seastar::future<RPCReadResult> {
          return backend.get(param.key).then(
              [param](std::string value) -> RPCReadResult {
                return RPCReadResult(ycsbc::DB::kOK, {{param.key, value}});
              });

#ifdef DEBUG
          debug << "READ " << param.table << ' ' << param.key;
          if (param.fields) {
            debug << " [ ";
            for (auto f : *param.fields) {
              debug << f << ' ';
            }
            debug << ']' << '\n';
          } else {
            debug << " < all fields >\n";
          }
#endif
        });

    myrpc.register_handler(2, [](RPCScanParam param) -> RPCScanResult {
#ifdef DEBUG
      debug << "SCAN " << param.table << ' ' << param.key << ' ' << param.len;
      if (param.fields) {
        debug << " [ ";
        for (auto f : *param.fields) {
          debug << f << ' ';
        }
        debug << ']' << '\n';
      } else {
        debug << " < all fields >\n";
      }
#endif
      return RPCScanResult();
    });

    myrpc.register_handler(3,
                           [](RPCMultiReadParam param) -> RPCMultiReadResult {
#ifdef DEBUG
                             debug << "MultiGet " << param.table << ' ';
                             debug << "keys:\t[ ";
                             for (auto &key : param.keys) {
                               debug << key << ' ';
                             }
                             debug << "]\n";
                             if (param.fields) {
                               debug << "fields:\t[ ";
                               // for (auto f : *fields) {
                               //   debug << f << ' ';
                               // }
                               debug << ']' << '\n';
                             } else {
                               debug << " < all fields >\n";
                             }
#endif
                             return RPCMultiReadResult();
                           });

    myrpc.register_handler(
        4, [this](RPCUpdateParam param) -> seastar::future<RPCUpdateResult> {
          return backend.set(param.key, param.values[0].second).then([] {
            return RPCUpdateResult(ycsbc::DB::kOK);
          });
#ifdef DEBUG
          debug << "UPDATE " << param.table << ' ' << param.key << " [ ";
          for (auto v : param.values) {
            debug << v.first << '=' << v.second << ' ';
          }
          debug << ']' << '\n';
#endif
        });

    myrpc.register_handler(
        5, [this](RPCInsertParam param) -> seastar::future<RPCInsertResult> {
          return backend.set(param.key, param.values[0].second).then([] {
            return RPCInsertResult(ycsbc::DB::kOK);
          });
#ifdef DEBUG
          debug << "INSERT " << param.table << ' ' << param.key << " [ ";
          for (auto v : param.values) {
            debug << v.first << '=' << v.second << ' ';
          }
          debug << ']' << '\n';
#endif
        });

    myrpc.register_handler(
        6, [this](RPCDeleteParam param) -> seastar::future<RPCDeleteResult> {
          return backend.remove(param.key).then(
              [] { return RPCDeleteResult(ycsbc::DB::kOK); });
#ifdef DEBUG
          debug << "DELETE " << param.table << ' ' << param.key << '\n';
#endif
        });

    (void)seastar::parallel_for_each(
        boost::irange<unsigned>(0, seastar::smp::count),
        [address, port, this](unsigned c) {
          return seastar::smp::submit_to(c, [address, port, this]() {
            static thread_local rpc::protocol<serializer>::server *server;

            rpc::resource_limits rl;
            rl.isolate_connection = [this](auto cookie) {
              return rpc::default_isolate_connection(cookie);
            };

            rpc::server_options so;
            so.filter_connection = [](const socket_address &sa) {
              return true;
            };

            server = new rpc::protocol<serializer>::server(
                myrpc, so, ipv4_addr{address, port}, rl);
            std::cerr << fmt::format("Server started on shard {}\n",
                                     seastar::this_shard_id());

            return seastar::make_ready_future<>();
          });
        });
  }
};

int main(int ac, char **av) {
  using namespace std::chrono_literals;
  app_template app;
  app.add_options()("address",
                    bpo::value<std::string>()->default_value("0.0.0.0"),
                    "Bind address");
  app.add_options()("port", bpo::value<ushort>()->default_value(10000),
                    "Bind port");

  return app.run_deprecated(ac, av, [&]() {
    auto &&config = app.configuration();

    std::string address = config["address"].as<std::string>();
    ushort port = config["port"].as<ushort>();

    return seastar::async([address, port]() -> void {
      RPCServer *server = new RPCServer(address, port);
    });
  });
}
