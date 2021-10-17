#include <iostream>

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>

#include <sys/types.h>

#include <rpc-proto.hh>
#include <serializer.hh>

using namespace std;
using namespace seastar;
namespace bpo = boost::program_options;

struct {
  template <typename T> auto &operator<<(T &&) { return *this; }
} debug;

class RPCServer {
  rpc::protocol<serializer> myrpc;

public:
  RPCServer(std::string address, ushort port) : myrpc(serializer{}) {
    myrpc.register_handler(1, [](RPCReadParam param) -> RPCReadResult {
      debug << "READ " << param.table << ' ' << param.key;
      if (param.fields) {
        debug << " [ ";
        for (auto f : *param.fields) {
          debug << f << ' ';
        }
        debug << ']' << "\n";
      } else {
        debug << " < all fields >" << "\n";
      }
      return RPCReadResult();
    });

    myrpc.register_handler(2, [](RPCScanParam param) -> RPCScanResult {
      debug << "SCAN " << param.table << ' ' << param.key << " " << param.len;
      if (param.fields) {
        debug << " [ ";
        for (auto f : *param.fields) {
          debug << f << ' ';
        }
        debug << ']' << "\n";
      } else {
        debug << " < all fields >" << "\n";
      }
      return RPCScanResult();
    });

    myrpc.register_handler(3,
                           [](RPCMultiReadParam param) -> RPCMultiReadResult {
                             debug << "MultiGet " << param.table << ' ';
                             debug << "keys:\t[ ";
                             for (auto &key : param.keys) {
                               debug << key << ' ';
                             }
                             debug << "]" << "\n";
                             if (param.fields) {
                               debug << "fields:\t[ ";
                               // for (auto f : *fields) {
                               //   debug << f << ' ';
                               // }
                               debug << ']' << "\n";
                             } else {
                               debug << " < all fields >" << "\n";
                             }
                             return RPCMultiReadResult();
                           });

    myrpc.register_handler(4, [](RPCUpdateParam param) -> RPCUpdateResult {
      debug << "UPDATE " << param.table << ' ' << param.key << " [ ";
      for (auto v : param.values) {
        debug << v.first << '=' << v.second << ' ';
      }
      debug << ']' << "\n";
      return RPCUpdateResult();
    });

    myrpc.register_handler(5, [](RPCInsertParam param) -> RPCInsertResult {
      debug << "INSERT " << param.table << ' ' << param.key << " [ ";
      for (auto v : param.values) {
        debug << v.first << '=' << v.second << ' ';
      }
      debug << ']' << "\n";
      return RPCInsertResult();
    });

    myrpc.register_handler(6, [](RPCDeleteParam param) -> RPCDeleteResult {
      debug << "DELETE " << param.table << ' ' << param.key << "\n";
      return RPCDeleteResult();
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
            std::cerr << "Server started on shard " << seastar::this_shard_id()
                      << std::endl;

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
