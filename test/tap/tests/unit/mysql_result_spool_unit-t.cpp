#include "MySQL_ResultSpool.h"
#include "tap.h"
#include "test_globals.h"
#include "test_init.h"
#include "MySQL_Protocol.h"
#include <chrono>
#include <dirent.h>
#include <functional>
#include <array>
#include <random>

static bool wait_until(const std::function<bool()>& predicate) {
 auto end = std::chrono::steady_clock::now() + std::chrono::seconds(10);
 while (!predicate()) {
  if (std::chrono::steady_clock::now() >= end) return false;
  std::this_thread::sleep_for(std::chrono::microseconds(50));
 }
 return true;
}

static size_t descriptors() {
 size_t count = 0;
 for (int fd = 0; fd < 4096; ++fd) if (fcntl(fd, F_GETFD) != -1) ++count;
 return count;
}

static bool transfer(MySQL_ResultSpool& spool, const std::vector<unsigned char>& input,
                     std::vector<unsigned char>& output, size_t memory_limit, uint32_t seed = 20260930) {
 std::mt19937 random(seed);
 size_t position = 0;
 bool bounded = true;
 bool done = wait_until([&] {
  if (position < input.size() && position - output.size() < 32 * 1024) {
   size_t offered = std::min<size_t>(1 + random() % 3001, input.size() - position);
   position += spool.append(input.data() + position, offered);
  }
  // Random delays emulate an independently paced, slow frontend.
  if ((random() % 4) == 0 || position == input.size()) {
   auto data = spool.take(); output.insert(output.end(), data.begin(), data.end());
  }
  bounded = bounded && spool.memory_bytes() <= 2 * memory_limit;
  return spool.error() || (position == input.size() && !spool.pending());
 });
 return done && bounded && !spool.error() && output == input;
}

int main() {
 plan(14);
 test_init_minimal();
 size_t initial_descriptors = descriptors();
 {
  MySQL_Protocol protocol;
  MySQL_ResultSet admin_result;
  admin_result.buffer_init(&protocol);
  PtrSizeArray packets;
  bool completed = admin_result.get_resultset(&packets);
  ok(!completed && !admin_result.myds && admin_result.transfer_started && !packets.len,
     "SQLite/admin buffer initialization does not dereference an uninitialized MySQL stream");
 }
 std::vector<unsigned char> input(2 * 1024 * 1024);
 for (size_t i = 0; i < input.size(); ++i) input[i] = (i * 37 + i / 251) % 256;
 {
  MySQL_ResultSpool spool("/tmp", 1024, 128 * 1024);
  std::vector<unsigned char> output;
  ok(transfer(spool, input, output, 1024), "random append/read boundaries retain exact bytes and bounded memory across disk ring wrap");
  bool cycles = true;
  for (int i = 0; i < 50; ++i) {
   std::vector<unsigned char> small(input.begin(), input.begin() + 2993), result;
   cycles = cycles && transfer(spool, small, result, 1024);
  }
  ok(cycles, "repeated drain/reset cycles preserve FIFO bytes");
 }
 {
  // Wire fixtures include text and binary rows, multi-result status, terminal
  // ERR and large payload blocks split independently of packet boundaries. No decoder or re-encoder is
  // involved: packet headers, IDs and every payload byte must be unchanged.
  std::vector<unsigned char> wire = {
   1,0,0,1,1, 5,0,0,2,0xfe,0,0,2,0,
   2,0,0,3,1,'x', 5,0,0,4,0xfe,0,0,10,0,
   1,0,0,5,1, 4,0,0,6,0,0,42,0,
   9,0,0,7,0xff,0x15,0x04,'#','H','Y','0','0','0'
  };
  wire.insert(wire.end(), input.begin(), input.end());
  MySQL_ResultSpool spool("/tmp", 4096, 0);
  std::vector<unsigned char> result;
  ok(transfer(spool, wire, result, 4096), "text/binary/multi-result/late-error response byte stream is unchanged");
 }
 {
  MySQL_ResultSpool spool("/this-directory-does-not-exist/proxysql", 1024, 0);
  ok(spool.append(input.data(), 20) == 20, "file creation happens asynchronously after accepting handoff");
  ok(wait_until([&] { return spool.error() != 0; }) && spool.error() == ENOENT,
     "missing spool directory produces sticky failure");
  ok(spool.append(input.data(), 20) == 0, "failed spool never accepts more bytes");
 }
 {
  MySQL_ResultSpool spool("/tmp", 1024, 19);
  ok(spool.append(input.data(), 20) == 0 && spool.error() == ENOSPC,
     "configured disk limit fails explicitly rather than truncating accepted response");
 }
 {
  MySQL_ResultSpool spool("/tmp", 1024, 0, 1000000);
  ok(spool.error() == EBADF && spool.append(input.data(), 20) == 0,
     "failed wake descriptor duplication is fatal rather than silently hanging");
 }
 {
  int pipefd[2]; bool created = pipe(pipefd) == 0;
  fcntl(pipefd[0], F_SETFL, O_NONBLOCK); fcntl(pipefd[1], F_SETFL, O_NONBLOCK);
  MySQL_ResultSpool spool("/tmp", 1024, 0, pipefd[1]);
  spool.append(input.data(), 20);
  unsigned char byte = 1;
  ok(created && wait_until([&] { return read(pipefd[0], &byte, 1) == 1; }) && byte == 0,
     "worker completion wakes the existing nonblocking event pipe");
  spool.cancel(); close(pipefd[0]); close(pipefd[1]);
 }
 // Wait for earlier destruction tasks so descriptor accounting isolates the
 // cancellation stress rather than counting unrelated test cleanup races.
 wait_until([&] { return descriptors() <= initial_descriptors; });
 size_t baseline = descriptors();
 for (int i = 0; i < 200; ++i) {
  MySQL_ResultSpool spool("/tmp", 1024, 0);
  spool.append(input.data(), 1024);
  spool.take(); spool.cancel();
 }
 ok(wait_until([&] { return descriptors() <= baseline; }),
    "cancel during queued IO releases descriptors without leaking session file lifetimes");
 // Every survivor uses the same two production IO workers. Repeatedly
 // cancelling predecessor FIFOs exercises queued tasks and notification FD
 // reuse while unrelated sessions continue producing and reading disk data.
 wait_until([&] { return descriptors() <= baseline; });
 std::atomic<unsigned int> started{0};
 std::array<unsigned char, 16> success{};
 std::vector<std::thread> clients;
 for (size_t client = 0; client < success.size(); ++client) {
  clients.emplace_back([&, client] {
   ++started;
   while (started.load() != success.size()) std::this_thread::yield();
   if (client % 4 == 0) {
    for (int replacement = 0; replacement < 30; ++replacement) {
     int notify[2];
     if (pipe(notify)) return;
     fcntl(notify[0], F_SETFL, O_NONBLOCK); fcntl(notify[1], F_SETFL, O_NONBLOCK);
     {
      MySQL_ResultSpool cancelled("/tmp", 1024, 65536, notify[1]);
      cancelled.append(input.data(), 1024); cancelled.take(); cancelled.cancel();
      // Closing after cancellation deliberately makes these descriptor numbers
      // available to another session while its old asynchronous task survives.
      close(notify[0]); close(notify[1]);
     }
    }
   }
   std::vector<unsigned char> own(512 * 1024 + client * 31), received;
   for (size_t i = 0; i < own.size(); ++i) own[i] = (i * 37 + client * 71 + i / 251) % 256;
   int notify[2];
   if (pipe(notify)) return;
   fcntl(notify[0], F_SETFL, O_NONBLOCK); fcntl(notify[1], F_SETFL, O_NONBLOCK);
   {
    MySQL_ResultSpool survivor("/tmp", 1024, 65536, notify[1]);
    success[client] = transfer(survivor, own, received, 1024, 20260930 + client * 193);
    survivor.cancel(); close(notify[0]); close(notify[1]);
   }
  });
 }
 for (auto& client : clients) client.join();
 ok(std::all_of(success.begin(), success.end(), [](unsigned char value) { return value != 0; }),
    "16 simultaneous spool sessions preserve their distinct bytes across ring wraps, cancellation and FD reuse");
 ok(wait_until([&] { return descriptors() <= baseline; }),
    "concurrent cancellation and replacement release every spool and wake descriptor");
 char directory[] = "/tmp/proxysql-spool-test-XXXXXX";
 bool created = mkdtemp(directory) != nullptr;
 {
  MySQL_ResultSpool spool(directory, 1024, 0);
  spool.append(input.data(), 20);
  created = created && wait_until([&] { return !spool.take().empty(); });
  DIR* dir = opendir(directory); size_t files = 0;
  if (dir) { while (auto* entry = readdir(dir)) if (entry->d_name[0] != '.') ++files; closedir(dir); }
  ok(created && dir && files == 0, "spool file is immediately unlinked while response is readable");
 }
 rmdir(directory);
 test_cleanup_minimal();
 return exit_status();
}
