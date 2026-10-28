#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void block_checkpoint(int fd) {
  const char *directory = getenv("ZERO_CHECKPOINT_STALL_DIR");
  if (!directory) return;
  char link[64], target[PATH_MAX], armed[PATH_MAX], entered[PATH_MAX], release[PATH_MAX];
  snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
  ssize_t length = readlink(link, target, sizeof(target) - 1);
  if (length < 0) return;
  target[length] = 0;
  const char *name = strrchr(target, '/');
  if (!name || strcmp(name, "/stall.db")) return;
  snprintf(armed, sizeof(armed), "%s/armed", directory);
  if (unlink(armed) != 0) return;
  snprintf(entered, sizeof(entered), "%s/entered", directory);
  snprintf(release, sizeof(release), "%s/release", directory);
  int marker = open(entered, O_CREAT | O_WRONLY, 0600);
  if (marker < 0) abort();
  close(marker);
  int gate = open(release, O_RDONLY);
  char byte;
  if (gate < 0 || read(gate, &byte, 1) != 1) abort();
  close(gate);
}

int fsync(int fd) {
  block_checkpoint(fd);
  int (*original)(int) = dlsym(RTLD_NEXT, "fsync");
  return original(fd);
}

int fdatasync(int fd) {
  block_checkpoint(fd);
  int (*original)(int) = dlsym(RTLD_NEXT, "fdatasync");
  return original(fd);
}
