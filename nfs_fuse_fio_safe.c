#define FUSE_USE_VERSION 31
#define _GNU_SOURCE

#include <fuse.h>
#include <nfsc/libnfs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <semaphore.h>
#include <sys/statvfs.h>
#include <signal.h> // Required for SIGPIPE

// --- Configuration ---
#define NUM_CONSUMERS 32          
#define CHUNK_SIZE (1024 * 1024) 
#define POOL_CHUNKS 2048         
#define RING_SIZE 4096           

// --- Globals ---
static char *memory_pool; 
static char *global_server = NULL;
static char *global_path = NULL;

// Contexts
static struct nfs_context *worker_ctx[NUM_CONSUMERS];
static struct nfs_context *main_ctx = NULL;
static pthread_mutex_t main_ctx_lock = PTHREAD_MUTEX_INITIALIZER;

// Flow Control
static sem_t sem_free_slots;
static sem_t sem_tasks_ready;
static atomic_int pending_write_count = 0; 

typedef struct {
    int slot_id;
    size_t size;
    off_t offset;
    uint64_t fh_ptr; 
} write_task_t;

struct ring_queue {
    write_task_t tasks[RING_SIZE];
    atomic_size_t head;
    atomic_size_t tail;
};

static struct ring_queue ring;
static atomic_bool running = true;
static pthread_t consumers[NUM_CONSUMERS];

// --- Helper: Ring Ops ---
static void push_task(write_task_t task) {
    size_t head = atomic_fetch_add(&ring.head, 1);
    ring.tasks[head % RING_SIZE] = task;
    sem_post(&sem_tasks_ready);
}

// --- Helper: Reconnection Logic (Blocking) ---
static void ensure_connected(int id) {
    // If we have a context, assume it's good for now.
    // If it's NULL (from a previous failure), we loop until we connect.
    while (worker_ctx[id] == NULL) {
        // Log only once per second to avoid spam
        static time_t last_log = 0;
        time_t now = time(NULL);
        if (now > last_log) {
            fprintf(stderr, "Worker %d: Attempting Reconnect...\n", id);
            last_log = now;
        }

        struct nfs_context *ctx = nfs_init_context();
        if (ctx == NULL) {
            sleep(1);
            continue;
        }

        if (nfs_mount(ctx, global_server, global_path) != 0) {
            nfs_destroy_context(ctx);
            sleep(1);
            continue;
        }

        // Optimization settings
        nfs_set_readahead(ctx, 16 * 1024 * 1024);
        
        // Success!
        worker_ctx[id] = ctx;
        fprintf(stderr, "Worker %d: Reconnected!\n", id);
    }
}

// --- Consumer Thread (Bulletproof Loop) ---
void *consumer_thread(void *arg) {
    int id = *(int*)arg;
    
    // Initial Connection
    ensure_connected(id);
    
    while (running) {
        sem_wait(&sem_tasks_ready);
        if (!running) break;

        size_t tail = atomic_fetch_add(&ring.tail, 1);
        write_task_t task = ring.tasks[tail % RING_SIZE];
        char *data_ptr = memory_pool + (task.slot_id * CHUNK_SIZE);
        
        // --- INFINITE RETRY LOOP ---
        // We will NOT proceed until this specific chunk is written.
        // Failing to write here corrupts the file.
        while (true) {
            ensure_connected(id);

            int ret = nfs_pwrite(worker_ctx[id], (struct nfsfh *)task.fh_ptr, task.offset, task.size, data_ptr);
            
            if (ret >= 0) {
                // Write Success! Break retry loop.
                break; 
            }
            
            // WRITE FAILED.
            // 1. Destroy broken context
            fprintf(stderr, "Worker %d: Write Error (%s). Reconnecting...\n", id, nfs_get_error(worker_ctx[id]));
            nfs_destroy_context(worker_ctx[id]);
            worker_ctx[id] = NULL;
            
            // 2. Sleep briefly to prevent CPU spin during network outage
            usleep(100000); // 100ms
            
            // 3. Loop continues -> ensure_connected() will be called next.
        }

        sem_post(&sem_free_slots);
        atomic_fetch_sub(&pending_write_count, 1);
    }
    return NULL;
}

// --- FUSE Init ---
static void *nfs_init(struct fuse_conn_info *conn, struct fuse_config *cfg) {
    conn->want |= FUSE_CAP_WRITEBACK_CACHE;
    conn->want |= FUSE_CAP_ASYNC_READ;
    conn->max_background = 128;
    conn->max_readahead = 32 * 1024 * 1024;

    memory_pool = aligned_alloc(4096, (size_t)POOL_CHUNKS * CHUNK_SIZE);
    if (!memory_pool) abort();

    sem_init(&sem_free_slots, 0, POOL_CHUNKS);
    sem_init(&sem_tasks_ready, 0, 0);
    atomic_init(&pending_write_count, 0);

    static int ids[NUM_CONSUMERS];
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        ids[i] = i;
        pthread_create(&consumers[i], NULL, consumer_thread, &ids[i]);
    }
    
    printf("NFS Driver Initialized. Bulletproof Mode.\n");
    return NULL;
}

// --- Write Ops (Lockless) ---
static int nfs_write_fuse(const char *path, const char *buf, size_t size,
                          off_t offset, struct fuse_file_info *fi) {
    size_t bytes_written = 0;
    static atomic_size_t slot_ticker = 0; 

    while (bytes_written < size) {
        size_t chunk = size - bytes_written;
        if (chunk > CHUNK_SIZE) chunk = CHUNK_SIZE;

        sem_wait(&sem_free_slots);

        size_t ticket = atomic_fetch_add(&slot_ticker, 1);
        int slot = ticket % POOL_CHUNKS;

        memcpy(memory_pool + (slot * CHUNK_SIZE), buf + bytes_written, chunk);
        atomic_fetch_add(&pending_write_count, 1);

        write_task_t task = {
            .slot_id = slot,
            .size = chunk,
            .offset = offset + bytes_written,
            .fh_ptr = fi->fh
        };
        push_task(task);
        bytes_written += chunk;
    }
    return size;
}

// --- Metadata Ops (LOCKED) ---

static int nfs_getattr_fuse(const char *path, struct stat *stbuf, struct fuse_file_info *fi) {
    (void) fi;
    struct nfs_stat_64 nst;
    pthread_mutex_lock(&main_ctx_lock);
    int ret = nfs_stat64(main_ctx, path, &nst);
    pthread_mutex_unlock(&main_ctx_lock);
    if (ret < 0) return ret;
    stbuf->st_mode = nst.nfs_mode; stbuf->st_nlink = nst.nfs_nlink;
    stbuf->st_size = nst.nfs_size; stbuf->st_uid = nst.nfs_uid;
    stbuf->st_gid = nst.nfs_gid; stbuf->st_atime = nst.nfs_atime;
    stbuf->st_mtime = nst.nfs_mtime; stbuf->st_ctime = nst.nfs_ctime;
    return 0;
}

static int nfs_open_fuse(const char *path, struct fuse_file_info *fi) {
    struct nfsfh *fh;
    pthread_mutex_lock(&main_ctx_lock);
    int ret = nfs_open(main_ctx, path, fi->flags, &fh);
    pthread_mutex_unlock(&main_ctx_lock);
    if (ret < 0) return ret;
    fi->fh = (uint64_t)fh;
    fi->keep_cache = 1;
    return 0;
}

static int nfs_create_fuse(const char *path, mode_t mode, struct fuse_file_info *fi) {
    struct nfsfh *fh;
    pthread_mutex_lock(&main_ctx_lock);
    int ret = nfs_creat(main_ctx, path, mode, &fh);
    pthread_mutex_unlock(&main_ctx_lock);
    if (ret < 0) return ret;
    fi->fh = (uint64_t)fh;
    return 0;
}

static int nfs_truncate_fuse(const char *path, off_t size, struct fuse_file_info *fi) {
    pthread_mutex_lock(&main_ctx_lock);
    int ret = nfs_truncate(main_ctx, path, size);
    pthread_mutex_unlock(&main_ctx_lock);
    return (ret < 0) ? ret : 0;
}

static int nfs_unlink_fuse(const char *path) { 
    pthread_mutex_lock(&main_ctx_lock);
    int ret = nfs_unlink(main_ctx, path);
    pthread_mutex_unlock(&main_ctx_lock);
    return (ret < 0) ? ret : 0;
}

static int nfs_mkdir_fuse(const char *path, mode_t mode) { 
    pthread_mutex_lock(&main_ctx_lock);
    int ret = nfs_mkdir(main_ctx, path);
    pthread_mutex_unlock(&main_ctx_lock);
    return (ret < 0) ? ret : 0;
}

static int nfs_rmdir_fuse(const char *path) { 
    pthread_mutex_lock(&main_ctx_lock);
    int ret = nfs_rmdir(main_ctx, path);
    pthread_mutex_unlock(&main_ctx_lock);
    return (ret < 0) ? ret : 0;
}

static int nfs_chmod_fuse(const char *path, mode_t mode, struct fuse_file_info *fi) {
    pthread_mutex_lock(&main_ctx_lock);
    int ret = nfs_chmod(main_ctx, path, mode);
    pthread_mutex_unlock(&main_ctx_lock);
    return (ret < 0) ? ret : 0;
}

static int nfs_chown_fuse(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi) {
    pthread_mutex_lock(&main_ctx_lock);
    int ret = nfs_chown(main_ctx, path, uid, gid);
    pthread_mutex_unlock(&main_ctx_lock);
    return (ret < 0) ? ret : 0;
}

static int nfs_utimens_fuse(const char *path, const struct timespec tv[2], struct fuse_file_info *fi) {
    struct timeval times[2];
    times[0].tv_sec = tv[0].tv_sec; times[0].tv_usec = tv[0].tv_nsec / 1000;
    times[1].tv_sec = tv[1].tv_sec; times[1].tv_usec = tv[1].tv_nsec / 1000;
    pthread_mutex_lock(&main_ctx_lock);
    int ret = nfs_utimes(main_ctx, path, times);
    pthread_mutex_unlock(&main_ctx_lock);
    return (ret < 0) ? ret : 0;
}

static int nfs_read_fuse(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    struct nfsfh *fh = (struct nfsfh *)fi->fh;
    pthread_mutex_lock(&main_ctx_lock);
    int ret = nfs_pread(main_ctx, fh, offset, size, buf);
    pthread_mutex_unlock(&main_ctx_lock);
    return ret;
}

static int nfs_readdir_fuse(const char *path, void *buf, fuse_fill_dir_t filler,
                            off_t offset, struct fuse_file_info *fi,
                            enum fuse_readdir_flags flags) {
    struct nfsdir *nfs_dir;
    struct nfsdirent *nfs_ent;
    pthread_mutex_lock(&main_ctx_lock);
    int ret = nfs_opendir(main_ctx, path, &nfs_dir);
    if (ret < 0) {
        pthread_mutex_unlock(&main_ctx_lock);
        return ret;
    }
    while((nfs_ent = nfs_readdir(main_ctx, nfs_dir)) != NULL) {
        if(filler(buf, nfs_ent->name, NULL, 0, 0)) break;
    }
    nfs_closedir(main_ctx, nfs_dir);
    pthread_mutex_unlock(&main_ctx_lock);
    return 0;
}

static void drain_writes() {
    while (atomic_load(&pending_write_count) > 0) usleep(1000); 
}

static int nfs_release_fuse(const char *path, struct fuse_file_info *fi) {
    struct nfsfh *fh = (struct nfsfh *)fi->fh;
    drain_writes(); 
    pthread_mutex_lock(&main_ctx_lock);
    if (fh) nfs_close(main_ctx, fh);
    pthread_mutex_unlock(&main_ctx_lock);
    return 0;
}

static int nfs_flush_fuse(const char *path, struct fuse_file_info *fi) {
    drain_writes(); 
    return 0;
}

static int nfs_fsync_fuse(const char *path, int isdatasync, struct fuse_file_info *fi) {
    drain_writes();
    return 0;
}

static struct fuse_operations nfs_oper = {
    .init = nfs_init,
    .getattr = nfs_getattr_fuse,
    .open = nfs_open_fuse,
    .read = nfs_read_fuse,
    .write = nfs_write_fuse,
    .create = nfs_create_fuse,
    .unlink = nfs_unlink_fuse,
    .mkdir = nfs_mkdir_fuse,
    .rmdir = nfs_rmdir_fuse,
    .readdir = nfs_readdir_fuse,
    .chmod = nfs_chmod_fuse,
    .chown = nfs_chown_fuse,
    .truncate = nfs_truncate_fuse,
    .utimens = nfs_utimens_fuse,
    .release = nfs_release_fuse,
    .flush = nfs_flush_fuse,
    .fsync = nfs_fsync_fuse,
};

int main(int argc, char *argv[]) {
    // 1. Ignore SIGPIPE to prevent crashes on network drops
    signal(SIGPIPE, SIG_IGN);

    main_ctx = nfs_init_context();
    char *url = getenv("NFS_URL");
    if (!url) { fprintf(stderr, "Set NFS_URL\n"); return 1; }

    // 2. Inject TCP Keepalive to prevent idle timeouts
    char robust_url[2048];
    char sep = strchr(url, '?') ? '&' : '?';
    snprintf(robust_url, sizeof(robust_url), "%s%ctcp-keepalive=1", url, sep);

    struct nfs_url *urls = nfs_parse_url_dir(main_ctx, robust_url);
    if (!urls) { fprintf(stderr, "Invalid URL\n"); return 1; }

    global_server = strdup(urls->server);
    global_path = strdup(urls->path);

    if (nfs_mount(main_ctx, global_server, global_path) != 0) {
        fprintf(stderr, "Mount failed: %s\n", nfs_get_error(main_ctx));
        return 1;
    }
    
    return fuse_main(argc, argv, &nfs_oper, NULL);
}
