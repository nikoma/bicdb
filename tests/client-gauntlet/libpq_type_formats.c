#include <arpa/inet.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct pg_conn PGconn;
typedef struct pg_result PGresult;

extern PGconn *PQconnectdb(const char *conninfo);
extern int PQstatus(const PGconn *conn);
extern char *PQerrorMessage(const PGconn *conn);
extern void PQfinish(PGconn *conn);
extern PGresult *PQexecParams(PGconn *conn, const char *command, int nParams,
                             const unsigned int *paramTypes,
                             const char *const *paramValues,
                             const int *paramLengths, const int *paramFormats,
                             int resultFormat);
extern int PQresultStatus(const PGresult *res);
extern char *PQresultErrorMessage(const PGresult *res);
extern int PQntuples(const PGresult *res);
extern int PQnfields(const PGresult *res);
extern char *PQgetvalue(const PGresult *res, int row_number, int column_number);
extern int PQgetlength(const PGresult *res, int row_number, int column_number);
extern int PQfformat(const PGresult *res, int column_number);
extern unsigned int PQftype(const PGresult *res, int column_number);
extern void PQclear(PGresult *res);

enum { CONNECTION_OK = 0, PGRES_TUPLES_OK = 2 };

static void fail(PGconn *conn, PGresult *result, const char *message) {
  fprintf(stderr, "%s: %s\n", message,
          result ? PQresultErrorMessage(result) : PQerrorMessage(conn));
  if (result) PQclear(result);
  PQfinish(conn);
  exit(1);
}

static int64_t read_i64(const unsigned char *bytes) {
  uint64_t value = 0;
  for (int i = 0; i < 8; i++) value = (value << 8) | bytes[i];
  return (int64_t)value;
}

static int16_t read_i16(const char *bytes) {
  uint16_t encoded;
  memcpy(&encoded, bytes, sizeof(encoded));
  return (int16_t)ntohs(encoded);
}

static int32_t read_i32(const char *bytes) {
  uint32_t encoded;
  memcpy(&encoded, bytes, sizeof(encoded));
  return (int32_t)ntohl(encoded);
}

int main(int argc, char **argv) {
  if (argc != 3) {
    fprintf(stderr, "usage: %s DATABASE_URL TARGET\n", argv[0]);
    return 2;
  }
  PGconn *conn = PQconnectdb(argv[1]);
  if (!conn || PQstatus(conn) != CONNECTION_OK) fail(conn, NULL, "libpq connect failed");

  const char *query =
      "SELECT TRUE::bool, (-12345)::int2, 123456789::int4, "
      "9007199254740993::int8, 'client-text'::text, '\\x00ff10'::bytea";
  const unsigned int expected_oids[] = {16, 21, 23, 20, 25, 17};
  PGresult *text = PQexecParams(conn, query, 0, NULL, NULL, NULL, NULL, 0);
  if (PQresultStatus(text) != PGRES_TUPLES_OK || PQntuples(text) != 1 ||
      PQnfields(text) != 6)
    fail(conn, text, "libpq text query failed");
  const char *expected_text[] = {"t", "-12345", "123456789", "9007199254740993",
                                 "client-text", "\\x00ff10"};
  for (int i = 0; i < 6; i++) {
    if (PQfformat(text, i) != 0 || PQftype(text, i) != expected_oids[i] ||
        strcmp(PQgetvalue(text, 0, i), expected_text[i]) != 0)
      fail(conn, text, "libpq text result mismatch");
  }
  PQclear(text);

  PGresult *binary = PQexecParams(conn, query, 0, NULL, NULL, NULL, NULL, 1);
  if (PQresultStatus(binary) != PGRES_TUPLES_OK || PQntuples(binary) != 1 ||
      PQnfields(binary) != 6)
    fail(conn, binary, "libpq binary query failed");
  for (int i = 0; i < 6; i++) {
    if (PQfformat(binary, i) != 1 || PQftype(binary, i) != expected_oids[i])
      fail(conn, binary, "libpq binary format/OID mismatch");
  }
  if (PQgetlength(binary, 0, 0) != 1 ||
      (unsigned char)PQgetvalue(binary, 0, 0)[0] != 1 ||
      PQgetlength(binary, 0, 1) != 2 ||
      read_i16(PQgetvalue(binary, 0, 1)) != -12345 ||
      PQgetlength(binary, 0, 2) != 4 ||
      read_i32(PQgetvalue(binary, 0, 2)) != 123456789 ||
      PQgetlength(binary, 0, 3) != 8 ||
      read_i64((const unsigned char *)PQgetvalue(binary, 0, 3)) != 9007199254740993LL ||
      PQgetlength(binary, 0, 4) != 11 ||
      memcmp(PQgetvalue(binary, 0, 4), "client-text", 11) != 0 ||
      PQgetlength(binary, 0, 5) != 3 ||
      memcmp(PQgetvalue(binary, 0, 5), "\x00\xff\x10", 3) != 0)
    fail(conn, binary, "libpq binary value mismatch");
  PQclear(binary);
  PQfinish(conn);
  printf("libpq text/binary gauntlet passed for %s\n", argv[2]);
  return 0;
}
