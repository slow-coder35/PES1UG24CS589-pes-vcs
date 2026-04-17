// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
//   100644 a1b2c3d4e5f6... 1699900000 42 README.md
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions implemented below: index_load, index_save, index_add

#include "index.h"
#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

/* ───────────────── PROVIDED ───────────────── */

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0) {
                memmove(&index->entries[i],
                        &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            }
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged = 0;

    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged++;
    }
    if (!staged) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged = 0;

    for (int i = 0; i < index->count; i++) {
        struct stat st;

        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged++;
        } else {
            if ((uint64_t)st.st_mtime != index->entries[i].mtime_sec ||
                (uint32_t)st.st_size != index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged++;
            }
        }
    }

    if (!unstaged) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked = 0;

    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;

        while ((ent = readdir(dir)) != NULL) {
            if (!strcmp(ent->d_name, ".") ||
                !strcmp(ent->d_name, "..") ||
                !strcmp(ent->d_name, ".pes") ||
                !strcmp(ent->d_name, "pes"))
                continue;

            if (strstr(ent->d_name, ".o")) continue;

            int tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (!strcmp(index->entries[i].path, ent->d_name)) {
                    tracked = 1;
                    break;
                }
            }

            if (!tracked) {
                struct stat st;
                if (stat(ent->d_name, &st) == 0 && S_ISREG(st.st_mode)) {
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked++;
                }
            }
        }
        closedir(dir);
    }

    if (!untracked) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

/* ───────────────── TODO IMPLEMENTED ───────────────── */

int index_load(Index *index) {
    index->count = 0;

    FILE *fp = fopen(INDEX_FILE, "r");
    if (!fp) return 0;   // empty index is fine

    char line[1024];

    while (fgets(line, sizeof(line), fp)) {
        if (index->count >= MAX_INDEX_ENTRIES) break;

        IndexEntry *e = &index->entries[index->count];

        char hash_hex[HASH_HEX_SIZE + 1];
        unsigned int mode;
        unsigned long mtime;
        unsigned int size;
        char path[512];

        int rc = sscanf(line, "%o %64s %lu %u %511[^\n]",
                        &mode, hash_hex, &mtime, &size, path);

        if (rc != 5) {
            fclose(fp);
            return -1;
        }

        e->mode = mode;
        e->mtime_sec = (uint64_t)mtime;
        e->size = size;

        if (hex_to_hash(hash_hex, &e->hash) != 0) {
            fclose(fp);
            return -1;
        }

        strncpy(e->path, path, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';

        index->count++;
    }

    fclose(fp);
    return 0;
}

static int cmp_entries(const void *a, const void *b) {
    const IndexEntry *x = (const IndexEntry *)a;
    const IndexEntry *y = (const IndexEntry *)b;
    return strcmp(x->path, y->path);
}

int index_save(const Index *index) {
    if (!index) return -1;

    FILE *fp = fopen(".pes/index.tmp", "w");
    if (!fp) return -1;

    for (int i = 0; i < index->count; i++) {
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&index->entries[i].hash, hex);

        fprintf(fp, "%o %s %lu %u %s\n",
                index->entries[i].mode,
                hex,
                (unsigned long)index->entries[i].mtime_sec,
                index->entries[i].size,
                index->entries[i].path);
    }

    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);

    if (rename(".pes/index.tmp", INDEX_FILE) != 0)
        return -1;

    return 0;
}

int index_add(Index *index, const char *path) {
    if (!index || !path) return -1;

    if (index->count < 0 || index->count > MAX_INDEX_ENTRIES)
        index->count = 0;

    struct stat st;
    if (stat(path, &st) != 0) return -1;

    FILE *fp = fopen(path, "rb");
    if (!fp) return -1;

    size_t sz = (size_t)st.st_size;
    void *buf = malloc(sz ? sz : 1);
    if (!buf) {
        fclose(fp);
        return -1;
    }

    if (sz > 0) {
        if (fread(buf, 1, sz, fp) != sz) {
            fclose(fp);
            free(buf);
            return -1;
        }
    }

    fclose(fp);

    ObjectID id;
    if (object_write(OBJ_BLOB, buf, sz, &id) != 0) {
        free(buf);
        return -1;
    }

    free(buf);

    IndexEntry *e = index_find(index, path);

    if (!e) {
        if (index->count >= MAX_INDEX_ENTRIES) return -1;
        e = &index->entries[index->count++];
    }

    e->mode = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;
    e->hash = id;
    e->mtime_sec = (uint64_t)st.st_mtime;
    e->size = (uint32_t)st.st_size;

    strncpy(e->path, path, sizeof(e->path) - 1);
    e->path[sizeof(e->path) - 1] = '\0';

    return index_save(index);
}