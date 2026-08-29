# Thrift phase probe refinement

## Goal

Refine the two broad Envoy Thrift intervals currently labelled request frame encoding and response
decoding, while adding sampled evidence that distinguishes full field decoding from payload
passthrough. The probes must not add per-request INFO logs or perform clock reads for unsampled
RPCs.

## Selected approach

Use the existing `KitexProbe` NDJSON trace path and add a lazy sampled-event macro. This preserves
trace correlation and lets the merge tool report non-overlapping wall-clock phases. Do not add
per-field timestamps or cumulative INFO logs: per-field clocks would perturb large structured
messages, while INFO output would distort performance runs.

Alternatives considered:

- Per-field decoder and encoder probes provide finer detail but scale with field count and are too
  intrusive for latency measurements.
- Periodic aggregate INFO statistics have lower trace volume but cannot correlate a phase with a
  sampled RPC and can still perturb the process when enabled.

## New sampled points

Request path:

- `up_body_start`: after upstream MessageBegin conversion and immediately before request body
  decoding continues.
- `up_frame_encode_start`: immediately before transport `encodeFrame`.
- `req_body_full_decode` or `req_body_passthrough`: one mutually exclusive path marker per sampled
  request.

Response path:

- `resp_transport_begin_done`: after TTHeader TransportBegin filters finish.
- `resp_message_begin_done`: after Binary MessageBegin metadata processing and filters finish.
- `resp_message_end_done`: after payload processing and MessageEnd filters finish.
- `resp_body_full_decode` or `resp_body_passthrough`: one mutually exclusive path marker per sampled
  response.

Existing points remain unchanged for compatibility: `up_encode_done`, `up_socket_write_done`,
`up_first_byte`, `resp_decoded`, and `dn_encode_done`.

## Phase definitions

Request-side reporting:

- `up_body_start -> up_frame_encode_start`: body decode, filters, protocol conversion, and payload
  output allocation; in passthrough mode this contains payload ownership movement instead.
- `up_frame_encode_start -> up_encode_done`: transport/TTHeader frame assembly.
- `up_encode_done -> up_socket_write_done`: connection write queue insertion.

Response-side reporting:

- `up_first_byte -> resp_transport_begin_done`: TTHeader decode and TransportBegin filters.
- `resp_transport_begin_done -> resp_message_begin_done`: Binary MessageBegin and metadata work.
- `resp_message_begin_done -> resp_message_end_done`: payload decode/filter/re-encode or passthrough.
- `resp_message_end_done -> resp_decoded`: frame end, TransportEnd filters, and finalization.
- `resp_decoded -> dn_encode_done`: downstream response frame encoding, retained as a separate
  existing phase.

## Overhead controls

- Add no new INFO log statements.
- Record the new points only after the connection is known to contain a sampled RPC.
- Avoid reading the monotonic clock for unbound or unsampled RPCs.
- Emit only one body-mode marker per direction per sampled RPC, guarded by a boolean in the active
  request/response object.
- Preserve the existing global probe-disable behavior and compile-time switch.

## Error and filter behavior

`*_done` points are emitted after the corresponding filter invocation returns. The body-mode marker
is emitted at the first full-decode `structBegin` callback or at the existing `passthroughData`
callback. Invalid or incomplete frames may therefore omit later points; the merge tool must skip
such incomplete intervals instead of filling them with zero.

## Verification

- Run format checks or at minimum `git diff --check` on the changed files.
- Build the affected Thrift proxy and merge targets when the configured Envoy build environment is
  available.
- Verify merge output with a synthetic trace containing every new point.
- On a short diagnostic run with `payload_passthrough: false`, require only the full-decode marker
  and zero native `request_passthrough`/`response_passthrough` counter deltas.
- On a short run with `payload_passthrough: true`, require the passthrough marker and positive native
  passthrough counter deltas.
- Performance runs keep INFO diagnostics disabled and use the existing sampled trace rate.
