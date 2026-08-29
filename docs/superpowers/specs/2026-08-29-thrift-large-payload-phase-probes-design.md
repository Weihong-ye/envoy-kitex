# Thrift large-payload phase probes

## Goal

Split the broad request frame-encoding and response-decoding intervals around the large Thrift
payload. The resulting trace must show whether a 32K, 64K, 128K, or 256K result comes from input
payload materialization, filter/protocol re-encoding into the output buffer, body-tail processing,
or TTHeader frame assembly.

This is diagnostic instrumentation only. It must not change Thrift semantics, enable payload
passthrough, change buffer ownership, or alter the existing 8K UBSocket block allocation policy.

## Selected approach

Instrument the first decoded Thrift string whose size is at least 4 KiB in each sampled request and
response. Capture a candidate start time in `fieldBegin`, when the field type is `String`, and emit
that saved timestamp only if the subsequent `stringValue` callback reports a value at least 4 KiB.
Record another point on entry to `stringValue` and a third after the decoder or encoder filter chain
returns.

This gives one representative large-payload interval per direction without adding a timestamp to
every field. It matches the Kitex echo workload, where the benchmark payload is carried by one large
Thrift string, and directly surrounds the code paths suspected of producing the size-dependent
latency difference.

Alternatives considered:

- Time every decoder event or string. This handles arbitrary schemas but adds clock reads in
  proportion to field count and can perturb the latency under investigation.
- Use only function sampling with `perf`. This identifies CPU hotspots but cannot correlate request
  payload materialization and output re-encoding within a single sampled RPC.
- Add periodic INFO counters. This loses phase timing and risks log-I/O distortion. No new INFO log
  is part of this design.

## Request points and intervals

Add these sampled points on the downstream request path:

- `req_payload_read_start`: the saved time immediately after the selected string field header has
  been decoded and its `FieldBegin` filters have returned, before the protocol reads the string
  length and bytes.
- `req_payload_read_done`: entry to `ActiveRpc::stringValue`, after the Binary protocol has
  completed its input-side `linearize` and `std::string::assign` work for the selected value.
- `req_payload_encode_done`: after `applyDecoderFilters(StringValue, ...)` returns. This includes
  request filters, conversion to the filter context string, Router/ProtocolConverter handling, and
  copying or allocating the re-encoded value into the request output buffer.
- `req_body_encode_done`: in `Router::messageEnd`, after `ProtocolConverter::messageEnd` and before
  `UpstreamRequest::encodeAndWrite`. This marks completion of all body-tail re-encoding.
- `req_passthrough_move_start` and `req_passthrough_move_done`: around the existing passthrough
  decoder-filter call. These replace the payload read/encode split when passthrough is configured.

The merge tool reports these non-overlapping request phases when their endpoints exist:

1. `up_body_start -> req_payload_read_start`: body prefix and field-header handling.
2. `req_payload_read_start -> req_payload_read_done`: selected large value materialization.
3. `req_payload_read_done -> req_payload_encode_done`: selected large value filters and output
   re-encoding.
4. `req_payload_encode_done -> req_body_encode_done`: remaining fields and body tail.
5. `req_body_encode_done -> up_frame_encode_start`: handoff and transport-buffer preparation.
6. `up_frame_encode_start -> up_encode_done`: TTHeader transport frame assembly.

In passthrough mode the tool reports `req_passthrough_move_start -> req_passthrough_move_done`
instead of inventing full-decode payload phases.

## Response points and intervals

Add the symmetric sampled points on the upstream response path:

- `resp_payload_read_start`: saved immediately after the selected string `FieldBegin` filters
  return.
- `resp_payload_read_done`: entry to `ResponseDecoder::stringValue`, after input materialization.
- `resp_payload_encode_done`: after the encoder-filter chain and ProtocolConverter have re-encoded
  the selected value into `response_buffer_`.
- `resp_passthrough_move_start` and `resp_passthrough_move_done`: around the existing passthrough
  encoder-filter call.

The merge tool reports:

1. `resp_message_begin_done -> resp_payload_read_start`: body prefix and field-header handling.
2. `resp_payload_read_start -> resp_payload_read_done`: selected large value materialization.
3. `resp_payload_read_done -> resp_payload_encode_done`: selected large value filters and output
   re-encoding.
4. `resp_payload_encode_done -> resp_message_end_done`: remaining fields, body tail, and MessageEnd.
5. `resp_message_end_done -> resp_decoded`: TransportEnd filters and response finalization before
   downstream TTHeader encoding.
6. `resp_decoded -> dn_encode_done`: downstream TTHeader frame assembly, retained from the existing
   trace.

In passthrough mode the tool reports the response passthrough move interval and omits unavailable
full-decode payload intervals.

## State and overhead controls

Each `ActiveRpc` and `ResponseDecoder` stores one optional candidate timestamp and one
`payload_recorded` bit. A `FieldBegin` callback updates the candidate only when all of the following
are true:

- the RPC is sampled by the existing Kitex probe;
- the field type is `String`;
- no qualifying payload has already been recorded.

`stringValue` consumes the candidate. If the value is smaller than 4 KiB, no point is emitted and a
later string may become the selected payload. Once a qualifying value is recorded, all payload
probe branches become no-ops for that direction.

No new environment variable or command-line option is added. Existing `KITEX_PROBE_PATH` and sample
rate control whether the detailed trace is collected. With the probe disabled or the RPC
unsampled, the new path performs no monotonic clock read, emits no event, and creates no log output.
Diagnostic runs may use a high sample rate; performance runs keep the existing low sample rate or
disable the probe entirely.

## Semantics and incomplete data

The probes observe existing execution and do not bypass `structBegin`, filters, or
ProtocolConverter. The 4 KiB threshold selects a measurement point; it does not affect allocation,
copying, framing, or protocol behavior.

A request without a string of at least 4 KiB has no payload triplet. A decoding error, stopped
filter chain, incomplete frame, or unsampled RPC may also omit later points. The merge tool must
skip an interval unless both endpoints exist and must never substitute zero. Existing body-mode
markers remain authoritative for distinguishing full decode from passthrough.

The selected read interval starts after `FieldBegin` processing. It therefore measures the large
value itself, not its field header. If data arrival is fragmented after the header, the interval can
include the wait until the value becomes available; this is intentional wall-clock behavior and
must be interpreted alongside read/socket phases.

## Verification

- Add unit coverage proving that a sampled 64K string emits exactly one request or response
  triplet, while small strings and unsampled RPCs do not.
- Cover full-decode and passthrough traces separately and verify they emit mutually appropriate
  points without changing output bytes.
- Run `git diff --check`, Envoy format checks for changed files, and the affected Thrift proxy
  tests.
- Update and test the merge tool with complete, missing-point, full-decode, and passthrough
  synthetic traces.
- Build the Release no-tcmalloc `envoy-static` binary and verify its version matches the committed
  source and contains the new point names.
- On the real Kitex echo workload, require zero payload verification mismatches before using phase
  results for performance attribution.

The first analysis compares baseline and zero-copy builds at identical payload, concurrency,
pinning, passthrough setting, sample rate, and UBSocket 8K block configuration. The result is
evidence of where wall time moved; a later allocator-versus-copy A/B is still required before
assigning an interval difference exclusively to memory-pool allocation or `memcpy`.
