/* dfltinaccel.c -- compress data using Intel PAC FPGAs

   Copyright © 2018-2021 InAccel

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.  */

#include <config.h>
#include <errno.h>
#include <inaccel/coral.h>
#include <unistd.h>

#include "gzip.h"

#define KVEC 16
#define KMINBUFFERSIZE 16384
#define MINIMUM_FILESIZE KVEC + 1

static size_t calcMaxTempSize(size_t in_size){
  size_t tmp_size = in_size + 16 * KVEC;
  tmp_size = tmp_size < KMINBUFFERSIZE ? KMINBUFFERSIZE : tmp_size;
  return(tmp_size);
}

static void crc_compute(const uch *in, size_t buffer_sz, ulg previous_crc) {
  const int num_nibbles_parallel = 64;
  const int num_sections = buffer_sz / (num_nibbles_parallel / 2);
  const uch *remaining_data = &in[num_sections * (num_nibbles_parallel / 2)];
  unsigned remaining_bytes = buffer_sz % (num_nibbles_parallel / 2);

  setcrc(previous_crc);
  updcrc(remaining_data, remaining_bytes);
}

struct GzipOutInfo {
  // final compressed block size
  size_t compression_sz;
  unsigned long crc;
};

static void inaccel_error(inaccel_request request, inaccel_response response)
{
    int errsv = errno;

    if (request) inaccel_request_release(request);
    if (response) inaccel_response_release(response);

    errno = errsv;
}

static int fpga_task(
    char *in, char *out, unsigned *crc, struct GzipOutInfo *gzip_info,
    size_t in_size, size_t out_size)
{
    size_t gzip_info_size = sizeof(struct GzipOutInfo);
    size_t crc_size = sizeof(unsigned);
    int last_block = 1;
    int nil = 0;

    inaccel_request request = inaccel_request_create("intel.compression.gzip");
    if (!request) return !OK;

    if (inaccel_request_arg_scalar(request, sizeof(size_t), &in_size, 0)) {
        inaccel_error(request, NULL);

        return !OK;
    }

    if (inaccel_request_arg_array(request, in_size, in, 1)) {
        inaccel_error(request, NULL);

        return !OK;
    }
    if (inaccel_request_arg_scalar(request, sizeof(size_t), &in_size, 2)){
        inaccel_error(request, NULL);

        return !OK;
    }
    if (inaccel_request_arg_scalar(request, sizeof(size_t), &nil, 3)){
        inaccel_error(request, NULL);

        return !OK;
    }
    if (inaccel_request_arg_array(request, out_size, out, 4)){
        inaccel_error(request, NULL);

        return !OK;
    }
    if (inaccel_request_arg_scalar(request, sizeof(size_t), &out_size, 5)){
        inaccel_error(request, NULL);

        return !OK;
    }
    if (inaccel_request_arg_scalar(request, sizeof(size_t), &nil, 6)){
        inaccel_error(request, NULL);

        return !OK;
    }
    if (inaccel_request_arg_array(
        request, sizeof(struct GzipOutInfo), gzip_info, 7))
    {
        inaccel_error(request, NULL);

        return !OK;
    }
    if (inaccel_request_arg_scalar(request, sizeof(size_t), &gzip_info_size, 8))
    {
        inaccel_error(request, NULL);

        return !OK;
    }
    if (inaccel_request_arg_scalar(request, sizeof(size_t), &nil, 9)){
        inaccel_error(request, NULL);

        return !OK;
    }
    if (inaccel_request_arg_array(request, sizeof(unsigned), crc, 10)){
        inaccel_error(request, NULL);

        return !OK;
    }
    if (inaccel_request_arg_scalar(request, sizeof(size_t), &crc_size, 11)){
        inaccel_error(request, NULL);

        return !OK;
    }
    if (inaccel_request_arg_scalar(request, sizeof(size_t), &nil, 12)){
        inaccel_error(request, NULL);

        return !OK;
    }
    if (inaccel_request_arg_scalar(request, sizeof(int), &last_block, 13)){
        inaccel_error(request, NULL);

        return !OK;
    }

    inaccel_response response = inaccel_response_create();
    if (!response) {
        inaccel_error(request, NULL);

        return !OK;
    }

    if (inaccel_submit(request, response)) {
        inaccel_error(request, response);

        return !OK;
    }

    inaccel_request_release(request);

    if (inaccel_response_wait(response)) {
        inaccel_error(NULL, response);

        return !OK;
    }

    inaccel_response_release(response);
    return OK;
}

/* ===========================================================================
 * Processes a new input file and return its compressed length. This
 * function performs fpga accelerated compression using Intel PAC FPGAs.
 */
size_t deflate_inaccel(int pack_level)
{
    if (ifile_size < MINIMUM_FILESIZE){
#ifdef IBM_Z_DFLTCC
        return dfltcc_deflate (pack_level);
#else
        return deflate (pack_level);
#endif
    }

    flush_outbuf(); // flush header contents to file

    off_t pos = lseek(ifd, 0, SEEK_CUR); // fd position for sw fallback
    if (pos == -1) {
        gzip_error ("Cannot resolve input file descriptor position\n");
    }

    char *in_buffer = (char *) inaccel_alloc(ifile_size * sizeof(char));
    size_t out_size = calcMaxTempSize(ifile_size);
    char *out_buffer = (char *) inaccel_alloc(out_size * sizeof(char));
    struct GzipOutInfo *gzip_info =
        (struct GzipOutInfo *) inaccel_alloc(sizeof(struct GzipOutInfo));
    unsigned *crc = (unsigned *) inaccel_alloc(sizeof(unsigned));

    if (!in_buffer || !out_buffer || !gzip_info || !crc) {
        fprintf(stderr, "%s: cannot allocate buffer(s) for FPGA\n",
                program_name);
#ifdef IBM_Z_DFLTCC
        return dfltcc_deflate(pack_level);
#else
        return deflate(pack_level);
#endif
    }

    off_t rb = read(ifd, in_buffer, ifile_size);
    if (rb == -1) {
        inaccel_free(crc);
        inaccel_free(gzip_info);
        inaccel_free(out_buffer);
        inaccel_free(in_buffer);

        read_error();
    }

    int ret = fpga_task(
        in_buffer, out_buffer, crc, gzip_info, ifile_size, out_size);
    if (ret == !OK) {
        // handle errno
        inaccel_free(crc);
        inaccel_free(gzip_info);
        inaccel_free(out_buffer);
        inaccel_free(in_buffer);

        pos = lseek(ifd, pos, SEEK_SET); // fd position for sw fallback
        if (pos == -1) {
            gzip_error ("Cannot set input file descriptor position\n");
        }
#ifdef IBM_Z_DFLTCC
        return dfltcc_deflate(pack_level);
#else
        return deflate(pack_level);
#endif
    }

    size_t len = gzip_info[0].compression_sz;
    off_t wb = write(ofd, out_buffer, gzip_info[0].compression_sz);

    crc_compute((uch *) in_buffer, ifile_size, crc[0]);

    inaccel_free(crc);
    inaccel_free(gzip_info);
    inaccel_free(out_buffer);
    inaccel_free(in_buffer);

    if (wb == -1) write_error();

    bytes_in += rb;
    bytes_out += wb;

    return len;
}
