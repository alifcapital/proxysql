#ifndef PROXYSQL_MYSQL_RESULT_SPOOL_H
#define PROXYSQL_MYSQL_RESULT_SPOOL_H

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>
#ifdef __linux__
#include <linux/falloc.h>
#include <sys/syscall.h>
#endif

/**
 * @brief Bounded asynchronous FIFO for an unchanged MySQL wire byte stream.
 * @details All filesystem operations run on two shared IO workers. The event
 * worker retains bytes not accepted by append() and applies backend pressure.
 * A finite disk limit uses a circular file; zero uses sparse prefix reclamation
 * on Linux and truncation whenever the disk reader catches up with the writer.
 */
class MySQL_ResultSpool {
 public:
 static constexpr size_t chunk_size = 64 * 1024;
 private:
 struct State {
  std::mutex mutex;
  std::deque<std::vector<unsigned char>> writes;
  std::vector<unsigned char> ready;
  std::string directory;
  size_t memory_limit, queued = 0;
  uint64_t disk_limit, read_offset = 0, write_offset = 0;
  int fd = -1, wake_fd = -1, error = 0;
  uint64_t reclaimed_offset = 0;
  bool scheduled = false, cancelled = false, notified = false;
  State(std::string dir, size_t memory, uint64_t disk, int wake)
   : directory(std::move(dir)), memory_limit(memory), disk_limit(disk), wake_fd(wake) {}
  ~State() { if (fd >= 0) close(fd); if (wake_fd >= 0) close(wake_fd); }
  void notify() {
   if (notified) return;
   notified = true;
   if (!cancelled && wake_fd >= 0) {
    unsigned char c = 0;
    // The session cancels under this mutex before its thread closes the pipe.
    // The duplicated nonblocking writer cannot be recycled beneath this task.
    ssize_t rc;
    do { rc = write(wake_fd, &c, 1); } while (rc < 0 && errno == EINTR);
   }
  }
 };
 class Executor {
  std::mutex mutex;
  std::condition_variable cv;
  std::deque<std::shared_ptr<State>> tasks;
  bool stopping = false;
  std::thread workers[2];
  static bool io(int fd, unsigned char* data, size_t size, uint64_t offset,
                 uint64_t limit, bool writing, int& error) {
   size_t done = 0;
   while (done < size) {
    uint64_t physical = limit ? (offset + done) % limit : offset + done;
    size_t n = size - done;
    if (limit) n = std::min<uint64_t>(n, limit - physical);
    ssize_t rc = writing ? pwrite(fd, data + done, n, physical)
                         : pread(fd, data + done, n, physical);
    if (rc < 0 && errno == EINTR) continue;
    if (rc <= 0) { error = rc < 0 ? errno : EIO; return false; }
    done += static_cast<size_t>(rc);
   }
   return true;
  }
  void process(const std::shared_ptr<State>& s) {
   std::unique_lock<std::mutex> lock(s->mutex);
   if (s->cancelled || s->error) {
    s->writes.clear(); s->queued = 0; s->ready.clear();
    if (s->fd >= 0) { int fd = s->fd; s->fd = -1; lock.unlock(); close(fd); lock.lock(); }
    s->scheduled = false; return;
   }
   if (s->fd < 0) {
    std::string path = s->directory + "/proxysql-result-XXXXXX";
    std::vector<char> name(path.begin(), path.end()); name.push_back(0);
    lock.unlock();
    #ifdef __linux__
    int fd = mkostemp(name.data(), O_CLOEXEC);
#else
    int fd = mkstemp(name.data());
#endif
    int error = fd < 0 ? errno : 0;
    if (fd >= 0) {
     if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) error = errno;
     int rc;
     do { rc = unlink(name.data()); } while (rc < 0 && errno == EINTR);
     if (rc < 0) { error = errno; unlink(name.data()); }
     if (error) { close(fd); fd = -1; }
    }
    lock.lock(); s->fd = fd; s->error = error;
   }
   if (!s->error && !s->cancelled && !s->writes.empty()) {
    auto data = std::move(s->writes.front()); s->writes.pop_front();
    uint64_t offset = s->write_offset;
    int error = 0;
    lock.unlock();
    bool success = io(s->fd, data.data(), data.size(), offset, s->disk_limit, true, error);
    lock.lock();
    s->queued -= data.size();
    if (success) s->write_offset += data.size(); else s->error = error;
   }
   if (!s->error && !s->cancelled && s->ready.empty() && s->write_offset > s->read_offset) {
    size_t size = std::min<uint64_t>(std::min(chunk_size, s->memory_limit), s->write_offset - s->read_offset);
    std::vector<unsigned char> data(size);
    uint64_t offset = s->read_offset;
    int error = 0;
    lock.unlock();
    bool success = io(s->fd, data.data(), size, offset, s->disk_limit, false, error);
    lock.lock();
    if (success) { s->read_offset += size; s->ready = std::move(data); }
    else s->error = error;
   }
   // Once disk catches up with the reader, reset physical offsets. Long-lived
   // connections therefore do not retain previous responses' disk allocation.
   if (!s->error && !s->cancelled && s->fd >= 0 && s->read_offset == s->write_offset && s->write_offset) {
    lock.unlock(); int rc = ftruncate(s->fd, 0); int error = errno; lock.lock();
    if (rc) s->error = error; else s->read_offset = s->write_offset = s->reclaimed_offset = 0;
   }
#ifdef __linux__
   // Free fully consumed extents even if a continuously streaming response
   // never drains. Sparse logical offsets can grow, but allocated disk space
   // tracks unread data. Unsupported filesystems retain safe FIFO behavior.
   if (!s->disk_limit && s->fd >= 0 && s->read_offset >= s->reclaimed_offset + 1024 * 1024) {
    uint64_t end = s->read_offset & ~uint64_t(4095);
    uint64_t start = s->reclaimed_offset;
    lock.unlock();
    syscall(SYS_fallocate, s->fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            static_cast<off_t>(start), static_cast<off_t>(end - start));
    lock.lock(); s->reclaimed_offset = end;
   }
#endif
   s->notify();
   bool again = s->cancelled || (!s->error && (!s->writes.empty() ||
                      (s->ready.empty() && s->write_offset > s->read_offset)));
   if (again) { lock.unlock(); submit(s); }
   else s->scheduled = false;
  }
  void run() {
   for (;;) {
    std::shared_ptr<State> s;
    { std::unique_lock<std::mutex> lock(mutex);
      cv.wait(lock, [&] { return stopping || !tasks.empty(); });
      if (tasks.empty() && stopping) return;
      s = std::move(tasks.front()); tasks.pop_front(); }
    process(s);
   }
  }
 public:
  Executor() { for (auto& worker : workers) worker = std::thread([this] { run(); }); }
  ~Executor() {
   { std::lock_guard<std::mutex> lock(mutex); stopping = true; }
   cv.notify_all(); for (auto& worker : workers) worker.join();
  }
  void submit(const std::shared_ptr<State>& s) {
   { std::lock_guard<std::mutex> lock(mutex); tasks.push_back(s); }
   cv.notify_one();
  }
 };
 std::shared_ptr<State> state;
 static Executor& executor() { static Executor value; return value; }
 void schedule_locked() {
  if (!state->scheduled) { state->scheduled = true; executor().submit(state); }
 }
 public:
 /**
  * @brief Configure an asynchronous, private, immediately unlinked spool.
  * @param memory_limit Maximum bytes in the write handoff, including active IO.
  * @param disk_limit Maximum unread disk bytes, or zero for no configured cap.
  * @param wake_fd Existing nonblocking event-pipe writer; duplicated with CLOEXEC.
  */
 MySQL_ResultSpool(const std::string& directory, size_t memory_limit, uint64_t disk_limit, int wake_fd = -1)
  : state(std::make_shared<State>(directory, std::max<size_t>(1, memory_limit), disk_limit,
                                wake_fd >= 0 ? fcntl(wake_fd, F_DUPFD_CLOEXEC, 0) : -1)) {
  if (wake_fd >= 0 && state->wake_fd < 0) state->error = errno;
 }
 ~MySQL_ResultSpool() { cancel(); }
 MySQL_ResultSpool(const MySQL_ResultSpool&) = delete;
 MySQL_ResultSpool& operator=(const MySQL_ResultSpool&) = delete;
 // Zero means bounded worker handoff is full, or a fatal error. Caller retains
 // ownership of unsent data and pauses backend fetching until completion wake.
 /** @brief Copy a bounded prefix; return bytes accepted, preserving FIFO order. */
 size_t append(const void* data, size_t size) {
  std::lock_guard<std::mutex> lock(state->mutex);
  if (state->error || state->cancelled) return 0;
  size_t n = std::min({size, chunk_size, state->memory_limit - state->queued});
  if (!n) return 0;
  uint64_t outstanding = state->write_offset - state->read_offset + state->queued;
  if (state->disk_limit && (n > state->disk_limit || outstanding > state->disk_limit - n)) {
   state->error = ENOSPC; state->notify(); return 0;
  }
  const auto* ptr = static_cast<const unsigned char*>(data);
  state->writes.emplace_back(ptr, ptr + n); state->queued += n;
  schedule_locked(); return n;
 }
 /** @brief Move the next bounded readback block to the event worker. */
 std::vector<unsigned char> take() {
  std::lock_guard<std::mutex> lock(state->mutex);
  auto result = std::move(state->ready); state->ready.clear();
  if (state->write_offset > state->read_offset || !state->writes.empty()) schedule_locked();
  return result;
 }
 /** @brief Consume the coalesced worker completion notification. */
 bool completed_event() {
  std::lock_guard<std::mutex> lock(state->mutex);
  bool result = state->notified; state->notified = false; return result;
 }
 /** @brief Whether buffered or committed response bytes remain unread. */
 bool pending() const {
  std::lock_guard<std::mutex> lock(state->mutex);
  return state->queued || !state->ready.empty() || state->write_offset != state->read_offset;
 }
 /** @brief Bytes in write handoff, in-flight IO, and bounded readback. */
 size_t memory_bytes() const {
  std::lock_guard<std::mutex> lock(state->mutex);
  return state->queued + state->ready.size();
 }
 /** @brief Sticky POSIX error; callers must abort the response on failure. */
 int error() const { std::lock_guard<std::mutex> lock(state->mutex); return state->error; }
 /** @brief Stop notifications and asynchronously release file resources. */
 void cancel() {
  std::lock_guard<std::mutex> lock(state->mutex);
  state->cancelled = true; schedule_locked();
 }
};
#endif
