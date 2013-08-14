#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <sys/sendfile.h>

static const size_t BLOCK_SZ = (1 << 20);

int main(int argc, char *argv[])
{
    if (argc != 2) {
        fprintf(stderr, "usage: %s OUTFILE\n", argv[0]);
        return 2;
    }

    const char *out_name = argv[1];

    int out_fd = creat(out_name, 0644);
    if (out_fd == -1) {
        fprintf(stderr, "Failed to open %s: %s\n", out_name, strerror(errno));
        return 1;
    }

    size_t total = 0;

    ssize_t rc;
    int i = 0;
    do {
        rc = sendfile(out_fd, STDIN_FILENO, NULL, BLOCK_SZ);
        if (rc == -1) {
            close(out_fd);
            fprintf(stderr, "sendfile (iter %d) failed: %s\n", i,
                    strerror(errno));
            return 1;
        }
        total += rc;
        ++i;
    }
    while (rc == BLOCK_SZ);

    printf("Wrote %zu bytes in %d sendfile call%s\n", total, i,
            (i == 1 ? "" : "s"));
}
