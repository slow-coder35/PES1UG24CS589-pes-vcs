// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
//   where <type> is "blob", "tree", or "commit"
//   and <size> is the decimal string of the data length
//
// Steps:
//   1. Build the full object: header ("blob 16\0") + data
//   2. Compute SHA-256 hash of the FULL object (header + data)
//   3. Check if object already exists (deduplication) — if so, just return success
//   4. Create shard directory (.pes/objects/XX/) if it doesn't exist
//   5. Write to a temporary file in the same shard directory
//   6. fsync() the temporary file to ensure data reaches disk
//   7. rename() the temp file to the final path (atomic on POSIX)
//   8. Open and fsync() the shard directory to persist the rename
//   9. Store the computed hash in *id_out

// HINTS - Useful syscalls and functions for this phase:
//   - sprintf / snprintf : formatting the header string
//   - compute_hash       : hashing the combined header + data
//   - object_exists      : checking for deduplication
//   - mkdir              : creating the shard directory (use mode 0755)
//   - open, write, close : creating and writing to the temp file
//                          (Use O_CREAT | O_WRONLY | O_TRUNC, mode 0644)
//   - fsync              : flushing the file descriptor to disk
//   - rename             : atomically moving the temp file to the final path
//

//
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str;
    char header[64];
    unsigned char *buf;
    size_t header_len, total_len;
    char path[512], dir[512], temp[512];
    char hex[HASH_HEX_SIZE + 1];
    int fd, dirfd;
    ssize_t written;

    if (type == OBJ_BLOB) type_str = "blob";
    else if (type == OBJ_TREE) type_str = "tree";
    else type_str = "commit";

    header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;
    total_len = header_len + len;

    buf = malloc(total_len);
    if (!buf) return -1;

    memcpy(buf, header, header_len);
    memcpy(buf + header_len, data, len);

    compute_hash(buf, total_len, id_out);

    if (object_exists(id_out)) {
        free(buf);
        return 0;
    }

    hash_to_hex(id_out, hex);

    snprintf(dir, sizeof(dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(OBJECTS_DIR, 0755);
    mkdir(dir, 0755);

    object_path(id_out, path, sizeof(path));
    snprintf(temp, sizeof(temp), "%s/tmpXXXXXX", dir);

    fd = mkstemp(temp);
    if (fd < 0) {
        free(buf);
        return -1;
    }

    written = write(fd, buf, total_len);
    if (written != (ssize_t)total_len) {
        close(fd);
        unlink(temp);
        free(buf);
        return -1;
    }

    fsync(fd);
    close(fd);

    if (rename(temp, path) != 0) {
        unlink(temp);
        free(buf);
        return -1;
    }

    dirfd = open(dir, O_RDONLY);
    if (dirfd >= 0) {
        fsync(dirfd);
        close(dirfd);
    }

    free(buf);
    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    FILE *fp;
    unsigned char *buf;
    long file_size;
    char *nul;
    char type_str[16];
    size_t size;
    ObjectID check;

    object_path(id, path, sizeof(path));

    fp = fopen(path, "rb");
    if (!fp) return -1;

    fseek(fp, 0, SEEK_END);
    file_size = ftell(fp);
    rewind(fp);

    if (file_size <= 0) {
        fclose(fp);
        return -1;
    }

    buf = malloc(file_size);
    if (!buf) {
        fclose(fp);
        return -1;
    }

    if (fread(buf, 1, file_size, fp) != (size_t)file_size) {
        fclose(fp);
        free(buf);
        return -1;
    }

    fclose(fp);

    compute_hash(buf, file_size, &check);
    if (memcmp(check.hash, id->hash, HASH_SIZE) != 0) {
        free(buf);
        return -1;
    }

    nul = memchr(buf, '\0', file_size);
    if (!nul) {
        free(buf);
        return -1;
    }

    if (sscanf((char *)buf, "%15s %zu", type_str, &size) != 2) {
        free(buf);
        return -1;
    }

    if (strcmp(type_str, "blob") == 0) *type_out = OBJ_BLOB;
    else if (strcmp(type_str, "tree") == 0) *type_out = OBJ_TREE;
    else if (strcmp(type_str, "commit") == 0) *type_out = OBJ_COMMIT;
    else {
        free(buf);
        return -1;
    }

    *data_out = malloc(size);
    if (!*data_out) {
        free(buf);
        return -1;
    }

    memcpy(*data_out, nul + 1, size);
    *len_out = size;

    free(buf);
    return 0;
}